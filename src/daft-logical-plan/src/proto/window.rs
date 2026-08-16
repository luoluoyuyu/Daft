use super::*;
pub(crate) fn window_expr_to_proto(window: &WindowExpr) -> DaftResult<proto::WindowExpr> {
    use proto::window_expr::Window as P;
    let window = match window {
        WindowExpr::Agg(agg) => P::Agg(Box::new(agg_expr_to_proto(agg)?)),
        WindowExpr::RowNumber => P::RowNumber(true),
        WindowExpr::Rank => P::Rank(true),
        WindowExpr::DenseRank => P::DenseRank(true),
        WindowExpr::Offset {
            input,
            offset,
            default,
        } => P::Offset(Box::new(proto::WindowOffsetExpr {
            input: Some(Box::new(expr_to_proto(input)?)),
            offset: *offset as i64,
            default: match default {
                Some(default) => Some(Box::new(expr_to_proto(default)?)),
                None => None,
            },
        })),
    };
    Ok(proto::WindowExpr {
        window: Some(window),
    })
}

pub(crate) fn window_expr_from_proto(window: proto::WindowExpr) -> DaftResult<WindowExpr> {
    use proto::window_expr::Window as P;
    match required(window.window, "WindowExpr oneof")? {
        P::Agg(agg) => Ok(WindowExpr::Agg(agg_expr_from_proto(*agg)?)),
        P::RowNumber(_) => Ok(WindowExpr::RowNumber),
        P::Rank(_) => Ok(WindowExpr::Rank),
        P::DenseRank(_) => Ok(WindowExpr::DenseRank),
        P::Offset(offset) => Ok(WindowExpr::Offset {
            input: required_expr(offset.input, "WindowOffsetExpr.input")?,
            offset: offset.offset as isize,
            default: match offset.default {
                Some(default) => Some(expr_from_proto(*default)?),
                None => None,
            },
        }),
    }
}

pub(crate) fn window_boundary_to_proto(boundary: &WindowBoundary) -> DaftResult<proto::WindowBoundary> {
    use proto::window_boundary::Boundary as P;
    let boundary = match boundary {
        WindowBoundary::UnboundedPreceding => P::UnboundedPreceding(true),
        WindowBoundary::UnboundedFollowing => P::UnboundedFollowing(true),
        WindowBoundary::Offset(offset) => P::Offset(*offset),
        WindowBoundary::RangeOffset(literal) => P::RangeOffset(literal.to_proto()?),
    };
    Ok(proto::WindowBoundary {
        boundary: Some(boundary),
    })
}

pub(crate) fn window_boundary_from_proto(boundary: proto::WindowBoundary) -> DaftResult<WindowBoundary> {
    use proto::window_boundary::Boundary as P;
    match required(boundary.boundary, "WindowBoundary oneof")? {
        P::UnboundedPreceding(_) => Ok(WindowBoundary::UnboundedPreceding),
        P::UnboundedFollowing(_) => Ok(WindowBoundary::UnboundedFollowing),
        P::Offset(offset) => Ok(WindowBoundary::Offset(offset)),
        P::RangeOffset(literal) => Ok(WindowBoundary::RangeOffset(literal.into_daft()?)),
    }
}

pub(crate) fn window_frame_to_proto(frame: &WindowFrame) -> DaftResult<proto::WindowFrame> {
    Ok(proto::WindowFrame {
        start: Some(window_boundary_to_proto(&frame.start)?),
        end: Some(window_boundary_to_proto(&frame.end)?),
    })
}

pub(crate) fn window_frame_from_proto(frame: proto::WindowFrame) -> DaftResult<WindowFrame> {
    Ok(WindowFrame {
        start: window_boundary_from_proto(required(frame.start, "WindowFrame.start")?)?,
        end: window_boundary_from_proto(required(frame.end, "WindowFrame.end")?)?,
    })
}

pub(crate) fn window_spec_to_proto(spec: &WindowSpec) -> DaftResult<proto::WindowSpec> {
    Ok(proto::WindowSpec {
        partition_by: spec
            .partition_by
            .iter()
            .map(expr_to_proto)
            .collect::<DaftResult<_>>()?,
        order_by: spec
            .order_by
            .iter()
            .map(expr_to_proto)
            .collect::<DaftResult<_>>()?,
        descending: spec.descending.clone(),
        nulls_first: spec.nulls_first.clone(),
        frame: match &spec.frame {
            Some(frame) => Some(window_frame_to_proto(frame)?),
            None => None,
        },
        min_periods: spec.min_periods as u64,
    })
}

pub(crate) fn window_spec_from_proto(spec: proto::WindowSpec) -> DaftResult<WindowSpec> {
    Ok(WindowSpec {
        partition_by: spec
            .partition_by
            .into_iter()
            .map(expr_from_proto)
            .collect::<DaftResult<_>>()?,
        order_by: spec
            .order_by
            .into_iter()
            .map(expr_from_proto)
            .collect::<DaftResult<_>>()?,
        descending: spec.descending,
        nulls_first: spec.nulls_first,
        frame: match spec.frame {
            Some(frame) => Some(window_frame_from_proto(frame)?),
            None => None,
        },
        min_periods: spec.min_periods as usize,
    })
}

