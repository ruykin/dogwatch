//! `goose harness` — bootstrap, submit, and inspect harness-managed sessions.
//!
//! `start` provisions a session from a profile (local path or control-plane
//! URL): applies locked config, materializes the workspace, writes the
//! harness context, then launches goose in the workspace as a child process.
//! The child (and any later goose invocation inside the workspace) discovers
//! the context and streams events; see `goose::harness`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Subcommand;

use goose::harness::{
    self, profile::Profile, HarnessContext, HARNESS_CONTEXT_ENV,
};

#[derive(Subcommand, Debug)]
pub enum HarnessCommand {
    /// Provision and launch a harness-managed session from a profile
    #[command(about = "Start a harness session from a profile (path or URL)")]
    Start {
        /// Profile source: local YAML path or http(s) URL
        #[arg(long, value_name = "PATH_OR_URL")]
        profile: String,

        /// Who is running this session (candidate id, email, SSO principal)
        #[arg(long, value_name = "ID")]
        principal: Option<String>,

        /// Bearer token for fetching the profile and authenticating ingest
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,

        /// Directory to materialize the workspace in (default: current dir)
        #[arg(long, value_name = "DIR")]
        dir: Option<PathBuf>,

        /// Provision only: set up the workspace and context without launching
        /// a session (any goose command run in the workspace attaches to it)
        #[arg(long)]
        no_launch: bool,
    },

    /// Submit the workspace state and close the harness session
    #[command(about = "Submit the final workspace state (git diff) and end the session")]
    Submit {
        /// Optional note recorded with the submission
        #[arg(long, value_name = "TEXT")]
        message: Option<String>,
    },

    /// Show the active harness session
    #[command(about = "Show the active harness session and event count")]
    Status {},
}

pub async fn handle_harness_command(command: HarnessCommand) -> Result<()> {
    match command {
        HarnessCommand::Start {
            profile,
            principal,
            token,
            dir,
            no_launch,
        } => start(profile, principal, token, dir, no_launch).await,
        HarnessCommand::Submit { message } => submit(message).await,
        HarnessCommand::Status {} => status(),
    }
}

async fn start(
    profile_source: String,
    principal: Option<String>,
    token: Option<String>,
    dir: Option<PathBuf>,
    no_launch: bool,
) -> Result<()> {
    let profile = Profile::load(&profile_source, token.as_deref()).await?;
    println!("Profile: {}", profile.id);
    if let Some(desc) = &profile.description {
        println!("  {desc}");
    }

    let base_dir = match dir {
        Some(d) => {
            std::fs::create_dir_all(&d)?;
            d.canonicalize()?
        }
        None => std::env::current_dir()?,
    };

    // Materialize workspace.
    let (workspace, base_commit) = match &profile.workspace {
        Some(spec) => {
            let dir_name = spec.dir.clone().unwrap_or_else(|| {
                spec.repo
                    .rsplit('/')
                    .next()
                    .unwrap_or("workspace")
                    .trim_end_matches(".git")
                    .to_string()
            });
            let target = base_dir.join(&dir_name);
            if target.join(".git").is_dir() {
                println!("Workspace already present: {}", target.display());
            } else {
                println!("Cloning {} -> {}", spec.repo, target.display());
                let mut args = vec!["clone".to_string(), spec.repo.clone()];
                if let Some(git_ref) = &spec.git_ref {
                    args.push("--branch".into());
                    args.push(git_ref.clone());
                }
                args.push(target.to_string_lossy().to_string());
                run_git(&base_dir, &args).await?;
            }
            let head = run_git(&target, &["rev-parse".into(), "HEAD".into()])
                .await
                .ok()
                .map(|out| out.trim().to_string());
            (target, head)
        }
        None => (base_dir.clone(), None),
    };

    // Locked config: applied as process env, the top of goose's config
    // precedence, and inherited by the child session process.
    apply_locked_config(&profile.config_locked);

    let mut ingest = profile.ingest.clone();
    if let (Some(ingest), Some(token)) = (ingest.as_mut(), token.as_ref()) {
        // The session token authenticates ingest when the profile doesn't
        // embed a credential of its own.
        if ingest.token.is_none() {
            ingest.token = Some(token.clone());
        }
    }

    // Keep harness state out of the candidate's git view and the submission.
    exclude_harness_dir(&workspace);

    let ctx = HarnessContext {
        harness_session_id: uuid::Uuid::now_v7().to_string(),
        principal,
        profile_id: Some(profile.id.clone()),
        workspace: workspace.clone(),
        base_commit,
        ingest,
        policy: profile.policy.clone(),
    };
    ctx.save()?;
    let context_path = ctx.context_path();
    // Make the context discoverable by the child process (and this one).
    std::env::set_var(HARNESS_CONTEXT_ENV, &context_path);

    let active = harness::activate(ctx).context("activating harness")?;
    active.emit(
        "session_start",
        None,
        serde_json::json!({
            "profile": profile.id,
            "workspace": workspace.to_string_lossy(),
            "goose_version": env!("CARGO_PKG_VERSION"),
        }),
    );
    active.flush().await;

    println!(
        "Harness session {} started (events: {})",
        active.context().harness_session_id,
        active.context().events_path().display()
    );

    if no_launch {
        println!(
            "Provisioned without launching. Run dogwatch inside {} (or `dogwatch harness submit` to finish).",
            workspace.display()
        );
        return Ok(());
    }

    // Launch goose in the workspace as a child so the session runs with the
    // full CLI experience while inheriting the harness context via env.
    let exe = std::env::current_exe().context("locating goose binary")?;
    let mut cmd = tokio::process::Command::new(exe);
    cmd.current_dir(&workspace);
    match &profile.recipe {
        Some(recipe) => {
            cmd.args(["run", "--recipe", recipe, "--interactive"]);
        }
        None => {
            cmd.arg("session");
        }
    }
    let status = cmd.status().await.context("launching goose session")?;

    active.emit(
        "session_pause",
        None,
        serde_json::json!({ "exit_code": status.code() }),
    );
    active.flush().await;
    println!(
        "Session paused. Run `dogwatch harness submit` in {} to finish, or `dogwatch session -r` to continue.",
        workspace.display()
    );
    Ok(())
}

async fn submit(message: Option<String>) -> Result<()> {
    let Some(active) = harness::active() else {
        bail!(
            "no active harness session found (looked for {} and .goose-harness/session.json in ancestors)",
            HARNESS_CONTEXT_ENV
        );
    };
    let ctx = active.context().clone();
    let workspace = &ctx.workspace;

    let mut payload = serde_json::Map::new();
    if let Some(msg) = message {
        payload.insert("message".into(), serde_json::json!(msg));
    }

    if workspace.join(".git").is_dir() {
        let base = ctx.base_commit.clone().unwrap_or_else(|| "HEAD".to_string());
        let diff = run_git(workspace, &["diff".into(), base.clone()])
            .await
            .unwrap_or_else(|e| format!("<git diff failed: {e}>"));
        let untracked = run_git(
            workspace,
            &[
                "ls-files".into(),
                "--others".into(),
                "--exclude-standard".into(),
            ],
        )
        .await
        .unwrap_or_default();
        payload.insert("base_commit".into(), serde_json::json!(base));
        payload.insert("diff".into(), serde_json::json!(cap_text(&diff)));
        payload.insert(
            "untracked_files".into(),
            serde_json::json!(untracked.lines().collect::<Vec<_>>()),
        );
    } else {
        payload.insert("note".into(), serde_json::json!("workspace is not a git repo; no diff captured"));
    }

    active.emit("artifact", None, serde_json::Value::Object(payload));
    active.emit(
        "session_end",
        None,
        serde_json::json!({ "reason": "submitted" }),
    );
    active.flush().await;

    println!(
        "Submitted harness session {}.",
        ctx.harness_session_id
    );
    println!("Local event log: {}", ctx.events_path().display());
    Ok(())
}

fn status() -> Result<()> {
    let Some(active) = harness::active() else {
        println!("No active harness session.");
        return Ok(());
    };
    let ctx = active.context();
    println!("Harness session: {}", ctx.harness_session_id);
    if let Some(p) = &ctx.principal {
        println!("Principal:       {p}");
    }
    if let Some(p) = &ctx.profile_id {
        println!("Profile:         {p}");
    }
    println!("Workspace:       {}", ctx.workspace.display());
    if let Some(ingest) = &ctx.ingest {
        println!("Ingest:          {}", ingest.endpoint);
    } else {
        println!("Ingest:          (local JSONL only)");
    }
    if !ctx.policy.is_empty() {
        println!(
            "Policy:          deny={:?} require_approval={:?}",
            ctx.policy.deny, ctx.policy.require_approval
        );
    }
    let events = std::fs::read_to_string(ctx.events_path())
        .map(|s| s.lines().count())
        .unwrap_or(0);
    println!(
        "Events:          {events} recorded at {}",
        ctx.events_path().display()
    );
    Ok(())
}

fn apply_locked_config(locked: &BTreeMap<String, String>) {
    for (key, value) in locked {
        let env_key = key.to_uppercase();
        std::env::set_var(&env_key, value);
        tracing::info!("harness: locked config {env_key}");
    }
}

/// Append `.goose-harness/` to the workspace's `.git/info/exclude` so harness
/// state never shows up as untracked candidate work.
fn exclude_harness_dir(workspace: &Path) {
    let git_dir = workspace.join(".git");
    if !git_dir.is_dir() {
        return;
    }
    let info_dir = git_dir.join("info");
    let exclude = info_dir.join("exclude");
    let entry = format!("{}/", goose::harness::HARNESS_DIR);
    let existing = std::fs::read_to_string(&exclude).unwrap_or_default();
    if existing.lines().any(|l| l.trim() == entry) {
        return;
    }
    let _ = std::fs::create_dir_all(&info_dir);
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&exclude)
        .map(|mut f| {
            use std::io::Write as _;
            let _ = writeln!(f, "{entry}");
        });
}

async fn run_git(dir: &Path, args: &[String]) -> Result<String> {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .await
        .context("running git")?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn cap_text(s: &str) -> String {
    const CAP: usize = 1_000_000;
    if s.len() > CAP {
        let mut end = CAP;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…<truncated {} bytes>", s.split_at(end).0, s.len() - end)
    } else {
        s.to_string()
    }
}
