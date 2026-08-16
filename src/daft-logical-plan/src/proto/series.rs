use super::*;
pub fn series_to_ipc(series: &Series) -> DaftResult<Vec<u8>> {
    use daft_recordbatch::RecordBatch;
    let field = series.field();
    let batch = RecordBatch::new_unchecked(
        Schema::new([Field::new(field.name.as_ref(), field.dtype.clone())]),
        vec![series.clone()],
        series.len(),
    );
    batch.to_ipc_stream()
}

pub fn series_from_ipc(bytes: &[u8]) -> DaftResult<Series> {
    use daft_recordbatch::RecordBatch;
    let batch = RecordBatch::from_ipc_stream(bytes)?;
    if batch.num_columns() != 1 {
        return invalid(format!(
            "expected single-column IPC payload for a Series literal, got {} columns",
            batch.num_columns()
        ));
    }
    Ok(batch.get_column(0).clone())
}

pub(crate) fn record_batch_to_ipc(batch: &RecordBatch) -> DaftResult<Vec<u8>> {
    batch.to_ipc_stream()
}

pub(crate) fn record_batch_from_ipc(bytes: &[u8]) -> DaftResult<RecordBatch> {
    RecordBatch::from_ipc_stream(bytes)
}

