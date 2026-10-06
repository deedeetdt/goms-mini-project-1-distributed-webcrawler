use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct WebStats {
    pub num_files: usize,
    pub num_exts: usize,
    pub ext_counts: HashMap<String, usize>,
    pub total_word_count: u64,
}
