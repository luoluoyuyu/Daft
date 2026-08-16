use super::*;
pub(crate) fn operator_to_proto(op: Operator) -> proto::Operator {
    use proto::Operator as P;
    match op {
        Operator::Eq => P::Eq,
        Operator::EqNullSafe => P::EqNullSafe,
        Operator::NotEq => P::NotEq,
        Operator::Lt => P::Lt,
        Operator::LtEq => P::LtEq,
        Operator::Gt => P::Gt,
        Operator::GtEq => P::GtEq,
        Operator::Plus => P::Plus,
        Operator::Minus => P::Minus,
        Operator::Multiply => P::Multiply,
        Operator::TrueDivide => P::TrueDivide,
        Operator::FloorDivide => P::FloorDivide,
        Operator::Modulus => P::Modulus,
        Operator::And => P::And,
        Operator::Or => P::Or,
        Operator::Xor => P::Xor,
        Operator::ShiftLeft => P::ShiftLeft,
        Operator::ShiftRight => P::ShiftRight,
    }
}

pub(crate) fn operator_from_proto(op: proto::Operator) -> DaftResult<Operator> {
    use proto::Operator as P;
    let op = match op {
        P::Eq => Operator::Eq,
        P::EqNullSafe => Operator::EqNullSafe,
        P::NotEq => Operator::NotEq,
        P::Lt => Operator::Lt,
        P::LtEq => Operator::LtEq,
        P::Gt => Operator::Gt,
        P::GtEq => Operator::GtEq,
        P::Plus => Operator::Plus,
        P::Minus => Operator::Minus,
        P::Multiply => Operator::Multiply,
        P::TrueDivide => Operator::TrueDivide,
        P::FloorDivide => Operator::FloorDivide,
        P::Modulus => Operator::Modulus,
        P::And => Operator::And,
        P::Or => Operator::Or,
        P::Xor => Operator::Xor,
        P::ShiftLeft => Operator::ShiftLeft,
        P::ShiftRight => Operator::ShiftRight,
        P::Unspecified => return invalid("Operator::Unspecified"),
    };
    Ok(op)
}

pub(crate) fn count_mode_to_proto(mode: CountMode) -> proto::CountMode {
    match mode {
        CountMode::All => proto::CountMode::All,
        CountMode::Valid => proto::CountMode::Valid,
        CountMode::Null => proto::CountMode::Null,
    }
}

pub(crate) fn count_mode_from_proto(mode: proto::CountMode) -> DaftResult<CountMode> {
    match mode {
        proto::CountMode::All => Ok(CountMode::All),
        proto::CountMode::Valid => Ok(CountMode::Valid),
        proto::CountMode::Null => Ok(CountMode::Null),
        proto::CountMode::Unspecified => invalid("CountMode::Unspecified"),
    }
}

pub(crate) fn join_side_to_proto(side: JoinSide) -> proto::JoinSide {
    match side {
        JoinSide::Left => proto::JoinSide::Left,
        JoinSide::Right => proto::JoinSide::Right,
    }
}

pub(crate) fn join_side_from_proto(side: proto::JoinSide) -> DaftResult<JoinSide> {
    match side {
        proto::JoinSide::Left => Ok(JoinSide::Left),
        proto::JoinSide::Right => Ok(JoinSide::Right),
        proto::JoinSide::Unspecified => invalid("JoinSide::Unspecified"),
    }
}

pub(crate) fn join_type_to_proto(join_type: daft_core::join::JoinType) -> proto::JoinType {
    use daft_core::join::JoinType as J;
    match join_type {
        J::Inner => proto::JoinType::Inner,
        J::Left => proto::JoinType::Left,
        J::Right => proto::JoinType::Right,
        J::Outer => proto::JoinType::Outer,
        J::Anti => proto::JoinType::Anti,
        J::Semi => proto::JoinType::Semi,
    }
}

pub(crate) fn join_type_from_proto(join_type: proto::JoinType) -> DaftResult<daft_core::join::JoinType> {
    use daft_core::join::JoinType as J;
    match join_type {
        proto::JoinType::Inner => Ok(J::Inner),
        proto::JoinType::Left => Ok(J::Left),
        proto::JoinType::Right => Ok(J::Right),
        proto::JoinType::Outer => Ok(J::Outer),
        proto::JoinType::Anti => Ok(J::Anti),
        proto::JoinType::Semi => Ok(J::Semi),
        proto::JoinType::Unspecified => invalid("JoinType::Unspecified"),
    }
}

pub(crate) fn join_strategy_to_proto(strategy: daft_core::join::JoinStrategy) -> proto::JoinStrategy {
    use daft_core::join::JoinStrategy as S;
    match strategy {
        S::Hash => proto::JoinStrategy::Hash,
        S::SortMerge => proto::JoinStrategy::SortMerge,
        S::Broadcast => proto::JoinStrategy::Broadcast,
        S::Cross => proto::JoinStrategy::Cross,
        S::KeyFiltering => proto::JoinStrategy::KeyFiltering,
    }
}

pub(crate) fn join_strategy_from_proto(
    strategy: proto::JoinStrategy,
) -> DaftResult<daft_core::join::JoinStrategy> {
    use daft_core::join::JoinStrategy as S;
    match strategy {
        proto::JoinStrategy::Hash => Ok(S::Hash),
        proto::JoinStrategy::SortMerge => Ok(S::SortMerge),
        proto::JoinStrategy::Broadcast => Ok(S::Broadcast),
        proto::JoinStrategy::Cross => Ok(S::Cross),
        proto::JoinStrategy::KeyFiltering => Ok(S::KeyFiltering),
        proto::JoinStrategy::Unspecified => invalid("JoinStrategy::Unspecified"),
    }
}

pub(crate) fn sketch_type_to_proto(sketch_type: SketchType) -> proto::SketchType {
    match sketch_type {
        SketchType::DDSketch => proto::SketchType::DdSketch,
        SketchType::HyperLogLog => proto::SketchType::HyperLogLog,
    }
}

pub(crate) fn sketch_type_from_proto(sketch_type: proto::SketchType) -> DaftResult<SketchType> {
    match sketch_type {
        proto::SketchType::DdSketch => Ok(SketchType::DDSketch),
        proto::SketchType::HyperLogLog => Ok(SketchType::HyperLogLog),
        proto::SketchType::Unspecified => invalid("SketchType::Unspecified"),
    }
}

pub(crate) fn on_error_to_proto(on_error: OnError) -> proto::OnError {
    match on_error {
        OnError::Raise => proto::OnError::Raise,
        OnError::Log => proto::OnError::Log,
        OnError::Ignore => proto::OnError::Ignore,
    }
}

pub(crate) fn on_error_from_proto(on_error: proto::OnError) -> DaftResult<OnError> {
    match on_error {
        proto::OnError::Raise => Ok(OnError::Raise),
        proto::OnError::Log => Ok(OnError::Log),
        proto::OnError::Ignore => Ok(OnError::Ignore),
        proto::OnError::Unspecified => invalid("OnError::Unspecified"),
    }
}

pub(crate) fn set_quantifier_to_proto(
    quantifier: crate::ops::SetQuantifier,
) -> proto::SetQuantifier {
    match quantifier {
        crate::ops::SetQuantifier::All => proto::SetQuantifier::All,
        crate::ops::SetQuantifier::Distinct => proto::SetQuantifier::Distinct,
    }
}

pub(crate) fn set_quantifier_from_proto(
    quantifier: proto::SetQuantifier,
) -> DaftResult<crate::ops::SetQuantifier> {
    match quantifier {
        proto::SetQuantifier::All => Ok(crate::ops::SetQuantifier::All),
        proto::SetQuantifier::Distinct => Ok(crate::ops::SetQuantifier::Distinct),
        proto::SetQuantifier::Unspecified => {
            invalid("SetQuantifier::Unspecified")
        }
    }
}

pub(crate) fn union_strategy_to_proto(strategy: crate::ops::UnionStrategy) -> proto::UnionStrategy {
    match strategy {
        crate::ops::UnionStrategy::Positional => proto::UnionStrategy::Positional,
        crate::ops::UnionStrategy::ByName => proto::UnionStrategy::ByName,
    }
}

pub(crate) fn union_strategy_from_proto(
    strategy: proto::UnionStrategy,
) -> DaftResult<crate::ops::UnionStrategy> {
    match strategy {
        proto::UnionStrategy::Positional => Ok(crate::ops::UnionStrategy::Positional),
        proto::UnionStrategy::ByName => Ok(crate::ops::UnionStrategy::ByName),
        proto::UnionStrategy::Unspecified => {
            invalid("UnionStrategy::Unspecified")
        }
    }
}
