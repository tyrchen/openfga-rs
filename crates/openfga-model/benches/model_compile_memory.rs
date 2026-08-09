//! Peak-heap gate for maximum-cardinality authorization-model compilation.

#![forbid(unsafe_code)]

use std::{error::Error, io};

use openfga_model::ModelCompiler;

mod support;

const MAXIMUM_COMPILATION_HEAP_BYTES: usize = 64 * 1024 * 1024;

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), Box<dyn Error>> {
    let source = support::maximum_supported_model()?;
    let profiler = dhat::Profiler::builder().testing().build();
    let compiled = ModelCompiler::default().compile(&source)?;
    let peak_bytes = dhat::HeapStats::get().max_bytes;
    if peak_bytes > MAXIMUM_COMPILATION_HEAP_BYTES {
        return Err(io::Error::other(format!(
            "maximum-model compilation peak heap was {peak_bytes} bytes, above \
             {MAXIMUM_COMPILATION_HEAP_BYTES} bytes",
        ))
        .into());
    }
    drop(compiled);
    drop(profiler);
    println!("maximum-model compilation peak heap: {peak_bytes} bytes");
    Ok(())
}
