#![cfg_attr(windows, windows_subsystem = "windows")]
use kafunekafka::ui::KafuneKafkaApp;
fn main() -> eframe::Result {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let args: Vec<String> = std::env::args().collect();
    let smoke = args
        .iter()
        .position(|a| a == "--smoke-ui")
        .and_then(|i| args.get(i + 1))
        .map(std::path::PathBuf::from);
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([1380., 900.])
            .with_active(smoke.is_none())
            .with_min_inner_size([1000., 700.]),
        renderer: eframe::Renderer::Glow,
        ..Default::default()
    };
    eframe::run_native(
        "kafunekafka",
        options,
        Box::new(move |cc| Ok(Box::new(KafuneKafkaApp::new(cc, smoke)))),
    )
}
