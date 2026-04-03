use crate::server::errors::{AppError, AppResult};
use error_stack::{ResultExt, bail};
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tracing::{error, info, warn};

/// 钩子步骤枚举：支持多种操作格式
/// 既支持 key-value 形式（如 {run: "..."}），也支持纯字符串（如 "rm"）
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HookStep {
    /// 执行命令格式：{run: "command"}
    Run { run: String },
    /// 移动文件格式：{mv: "target_dir"}
    Move { mv: String },
    /// Webhook 推送格式：{webhook: "https://example.com/notify"}
    /// 上传完成后以 POST 方式将稿件元数据（JSON）发送到指定 URL
    Webhook { webhook: String },
    /// 删除文件格式："rm"
    Remove(String),
}

impl HookStep {
    /// 执行钩子步骤操作
    ///
    /// # 参数
    /// * `video_paths` - 视频文件路径列表
    ///
    /// # 返回
    /// 执行成功返回Ok(())，失败返回错误信息
    pub async fn execute(&self, video_paths: &[&Path]) -> AppResult<()> {
        match self {
            HookStep::Run { run } => {
                // 执行自定义命令
                let paths_str = video_paths
                    .iter()
                    .map(|p| p.to_string_lossy())
                    .reduce(|acc, e| Cow::from(acc.to_string() + "\n" + &*e))
                    .ok_or(AppError::Unknown)?;
                self.execute_command(run, paths_str.as_bytes()).await?;
            }
            HookStep::Move { mv } => {
                // 移动文件到指定目录
                self.move_file(video_paths, mv).await?;
            }
            HookStep::Webhook { webhook } => {
                // 仅携带视频文件路径列表的推送（无上传元数据时）
                let videos: Vec<Value> = video_paths
                    .iter()
                    .map(|p| Value::String(p.to_string_lossy().to_string()))
                    .collect();
                let payload = serde_json::json!({ "videos": videos });
                self.post_webhook(webhook, &payload).await?;
            }
            HookStep::Remove(cmd) if cmd == "rm" => {
                // 删除文件
                HookStep::remove_file(video_paths).await?;
            }
            HookStep::Remove(cmd) => {
                // 未知命令，返回错误
                bail!(AppError::Custom(format!("Unknown command: {}", cmd)));
            }
        }
        Ok(())
    }

    /// 执行钩子步骤操作
    ///
    /// # 参数
    /// * `video_paths` - 视频文件路径列表
    ///
    /// # 返回
    /// 执行成功返回Ok(())，失败返回错误信息
    pub async fn execute_with(&self, src: &[u8]) -> AppResult<()> {
        match self {
            HookStep::Run { run } => {
                self.execute_command(run, src).await?;
            }
            HookStep::Webhook { webhook } => {
                // 尝试将 src 解析为 JSON；失败时包装成 {"data": "<string>"} 再推送
                let payload: Value = serde_json::from_slice(src).unwrap_or_else(|_| {
                    let text = String::from_utf8_lossy(src).to_string();
                    serde_json::json!({ "data": text })
                });
                self.post_webhook(webhook, &payload).await?;
            }
            cmd => {
                // 未知命令，返回错误
                bail!(AppError::Custom(format!("不支持的命令: {:?}", cmd)));
            }
        }
        Ok(())
    }

    /// 执行钩子步骤，将元数据（含 bvid/aid/title 等）与视频路径合并为 JSON 后通过 stdin 传入
    ///
    /// 对于 Run 类型：stdin 传入形如 `{"bvid":"BVxx","aid":123,"title":"...","videos":["/path/a.mp4"]}` 的 JSON
    /// 对于 Webhook 类型：以 POST 方式将同样格式的 JSON 发送到指定 URL
    /// 对于 Move/Remove 类型：行为与 `execute` 相同，忽略元数据
    ///
    /// # 参数
    /// * `video_paths` - 视频文件路径列表
    /// * `meta` - 上传成功后从 B 站 API 返回的元数据（如 bvid、aid、title 等）
    pub async fn execute_with_meta(&self, video_paths: &[&Path], meta: &Value) -> AppResult<()> {
        match self {
            HookStep::Run { run } => {
                let mut json = meta.clone();
                if let Some(obj) = json.as_object_mut() {
                    let videos: Vec<Value> = video_paths
                        .iter()
                        .map(|p| Value::String(p.to_string_lossy().to_string()))
                        .collect();
                    obj.insert("videos".to_string(), Value::Array(videos));
                }
                let json_str =
                    serde_json::to_string(&json).change_context(AppError::Unknown)?;
                self.execute_command(run, json_str.as_bytes()).await?;
            }
            HookStep::Webhook { webhook } => {
                // 将元数据与视频路径合并后 POST 到指定 URL
                let mut json = meta.clone();
                if let Some(obj) = json.as_object_mut() {
                    let videos: Vec<Value> = video_paths
                        .iter()
                        .map(|p| Value::String(p.to_string_lossy().to_string()))
                        .collect();
                    obj.insert("videos".to_string(), Value::Array(videos));
                }
                self.post_webhook(webhook, &json).await?;
            }
            HookStep::Move { mv } => {
                self.move_file(video_paths, mv).await?;
            }
            HookStep::Remove(cmd) if cmd == "rm" => {
                HookStep::remove_file(video_paths).await?;
            }
            HookStep::Remove(cmd) => {
                bail!(AppError::Custom(format!("不支持的命令: {:?}", cmd)));
            }
        }
        Ok(())
    }

    /// 以 HTTP POST 方式将 JSON 有效载荷推送到指定 Webhook URL
    ///
    /// # 参数
    /// * `url`     - 目标 Webhook 地址
    /// * `payload` - 要发送的 JSON 数据
    async fn post_webhook(&self, url: &str, payload: &Value) -> AppResult<()> {
        let body = serde_json::to_string(payload).change_context(AppError::Unknown)?;
        let client = reqwest::Client::new();
        let resp = client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .change_context(AppError::Unknown)?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.is_success() {
            info!(status=%status, response=%text, url=%url, "Webhook 推送成功");
        } else {
            warn!(status=%status, response=%text, url=%url, "Webhook 返回非 2xx 状态码");
        }
        Ok(())
    }

    /// 执行自定义命令，将视频路径作为标准输入传入
    ///
    /// # 参数
    /// * `cmd` - 要执行的命令字符串
    /// * `video_paths` - 视频文件路径列表
    async fn execute_command(&self, cmd: &str, src: &[u8]) -> AppResult<()> {
        // Windows 使用 "cmd /C"，Unix/Mac 使用 "sh -c"
        let (shell, flag) = if cfg!(target_os = "windows") {
            ("cmd", "/C")
        } else {
            ("sh", "-c")
        };
        // 执行自定义命令
        // 解析命令和参数
        // 启动子进程，配置标准输入管道
        let mut process = Command::new(shell)
            .arg(flag)
            .arg(cmd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .change_context(AppError::Unknown)?;

        // 将自定义输入写入标准输入
        if let Some(mut stdin) = process.stdin.take() {
            stdin
                .write_all(src)
                .await
                .change_context(AppError::Unknown)?;
        }

        let stdout = process.stdout.take().unwrap();
        let stderr = process.stderr.take().unwrap();

        let mut stdout_lines = BufReader::new(stdout).lines();
        let mut stderr_lines = BufReader::new(stderr).lines();

        loop {
            tokio::select! {
                        line = stdout_lines.next_line() => {
                            match line.change_context(AppError::Unknown)? {
                                Some(l) => tracing::info!(target="user_cmd_stdout", "{}", l),
                                None => break, // stdout EOF
                            }
                        }
                        line = stderr_lines.next_line() => {
                            match line.change_context(AppError::Unknown)? {
                                Some(l) => tracing::warn!(target="user_cmd_stderr", "{}", l),
                                None => break, // stderr EOF
                            }
                        }
                    }
        }

        // 等待进程完成并检查退出状态
        let status = process.wait().await.change_context(AppError::Unknown)?;

        if !status.success() {
            bail!(AppError::Custom(format!(
                        "Command failed with status: {}",
                        status
                    )));
        }

        Ok(())
    }

    /// 移动文件到指定目录
    ///
    /// # 参数
    /// * `video_paths` - 视频文件路径列表
    /// * `target_dir` - 目标目录路径
    async fn move_file(&self, video_paths: &[&Path], target_dir: &str) -> AppResult<()> {
        let target_path = Path::new(target_dir);

        if !target_path.exists() {
            fs::create_dir_all(target_path)
                .await
                .change_context(AppError::Unknown)?;
        }

        for video_path in video_paths {
            self.move_single_file(video_path, target_path).await?;

            // 移动对应的 XML 文件
            let xml_path = video_path.with_extension("xml");
            if xml_path.exists() {
                info!("移动弹幕文件: {}", xml_path.display());
                self.move_single_file(&xml_path, target_path).await?;
            }
        }
        Ok(())
    }

    /// 移动单个文件，支持跨文件系统
    async fn move_single_file(&self, source: &Path, target_dir: &Path) -> AppResult<()> {
        let file_name = source
            .file_name()
            .ok_or(AppError::Custom("Invalid file name".to_string()))?;
        let destination = target_dir.join(file_name);

        info!("Moving {} to {}", source.display(), destination.display());

        // 先尝试 rename（快速，仅同文件系统）
        match fs::rename(source, &destination).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // 检查是否是跨文件系统错误
                if HookStep::is_cross_device_error(&e) {
                    info!("检测到跨文件系统操作，使用复制+删除模式");
                    // 复制文件
                    fs::copy(source, &destination)
                        .await
                        .change_context(AppError::Unknown)?;

                    // 删除原文件
                    fs::remove_file(source)
                        .await
                        .change_context(AppError::Unknown)?;

                    Ok(())
                } else {
                    Err(e).change_context(AppError::Unknown)?
                }
            }
        }
    }

    /// 判断是否是跨设备/文件系统错误
    fn is_cross_device_error(error: &std::io::Error) -> bool {
        #[cfg(unix)]
        {
            error.raw_os_error() == Some(libc::EXDEV)
        }
        #[cfg(windows)]
        {
            error.raw_os_error() == Some(17) // ERROR_NOT_SAME_DEVICE
        }
        #[cfg(not(any(unix, windows)))]
        {
            error.kind() == std::io::ErrorKind::Other
        }
    }

    /// 删除指定的视频文件
    ///
    /// # 参数
    /// * `video_paths` - 要删除的视频文件路径列表
    pub(crate) async fn remove_file(video_paths: &[&Path]) -> AppResult<()> {
        // 逐个删除视频文件
        for video_path in video_paths {
            info!("删除 - Removing: {}", video_path.display());
            fs::remove_file(video_path)
                .await
                .change_context(AppError::Unknown)?;
            let buf = video_path.with_extension("xml");
            if buf.exists() {
                fs::remove_file(&buf)
                    .await
                    .change_context(AppError::Unknown)?;
                info!("删除弹幕文件 - Removing: {}", buf.display());
            }
        }
        Ok(())
    }
}

/// 处理所有后处理器步骤
/// 处理视频文件的主函数
/// 按顺序执行所有处理器步骤
///
/// # 参数
/// * `video_path` - 视频文件路径列表
/// * `processors` - 处理器步骤列表
pub async fn process_video(video_path: &[&Path], processors: &[HookStep]) -> AppResult<()> {
    info!("Starting video processing...");

    // 依次执行每个处理器步骤
    for processor in processors {
        processor.execute(video_path).await?;
    }

    info!("Video processing completed");
    Ok(())
}

/// 处理所有后处理器步骤，并将 B 站上传元数据（含 bvid/aid/title 等）传递给 Run 类型的钩子
///
/// Run 类型的钩子收到的 stdin 为 JSON 格式，包含 API 返回的所有字段以及 `videos` 数组（文件路径列表）
///
/// # 参数
/// * `video_paths` - 视频文件路径列表
/// * `meta` - 上传成功后从 B 站 API 返回的元数据
/// * `processors` - 处理器步骤列表
pub async fn process_video_with_meta(
    video_paths: &[&Path],
    meta: &Value,
    processors: &[HookStep],
) -> AppResult<()> {
    info!("Starting video processing with upload meta...");

    for processor in processors {
        processor.execute_with_meta(video_paths, meta).await?;
    }

    info!("Video processing with meta completed");
    Ok(())
}

pub async fn process(input: &[u8], processors: &Option<Vec<HookStep>>) {
    if let Some(hooks) = processors {
        // 依次执行每个处理器步骤
        for processor in hooks {
            info!(processor=?processor, "Starting processing...");
            if let Err(e) = processor.execute_with(input).await {
                error!(error=?e, "自定义处理执行出错");
            }
            info!(processor=?processor, "processing completed");
        }
    }
}
