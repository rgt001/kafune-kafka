//! Native GUI runner for test evidence. Uses the production UI and worker path.
use kafka_lens::ui::KafkaLensApp;
fn main() -> eframe::Result {
    let output = std::env::args().nth(1).expect("screenshot output path");
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([1380., 900.])
            .with_active(false),
        renderer: eframe::Renderer::Glow,
        ..Default::default()
    };
    eframe::run_native(
        "KafkaLens GUI benchmark",
        options,
        Box::new(move |cc| Ok(Box::new(KafkaLensApp::new(cc, Some(output.into()))))),
    )
}
