mod model;

use model::WebStats;

fn main() {
    let stats = WebStats::default();

    println!(
        "files: {}   extensions: {}   words: {}",
        stats.num_files, stats.num_exts, stats.total_word_count
    );
    println!("extension counts: {:?}", stats.ext_counts);
}
