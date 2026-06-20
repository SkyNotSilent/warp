mod telemetry;
mod transition;

pub use telemetry::LifecycleRecoveryRecord;
pub(in crate::terminal) use telemetry::LifecycleTelemetryEvent;
use telemetry::LifecycleTelemetryLimiter;
pub(in crate::terminal) use transition::{
    CommandStartKind, IgnoreReason, LifecycleAction, LifecycleInput, LifecyclePhase,
    LifecycleSnapshot, LifecycleTransition, NextBlockIdDisposition, PreexecObservation,
};
use warp_core::features::FeatureFlag;

use super::block::BlockState;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartCommandOutcome {
    Accepted,
    Coalesced,
    RejectedExecuting,
    IgnoredTerminated,
}

impl StartCommandOutcome {
    pub fn is_accepted(self) -> bool {
        matches!(self, StartCommandOutcome::Accepted)
    }
}

pub(super) struct BlockLifecycleCoordinator {
    phase: LifecyclePhase,
    epoch: u64,
    telemetry_limiter: LifecycleTelemetryLimiter,
}

impl Default for BlockLifecycleCoordinator {
    fn default() -> Self {
        Self {
            phase: LifecyclePhase::Unknown,
            epoch: 0,
            telemetry_limiter: LifecycleTelemetryLimiter::default(),
        }
    }
}

impl BlockLifecycleCoordinator {
    pub(super) fn plan(
        &mut self,
        snapshot: &LifecycleSnapshot,
        input: LifecycleInput,
    ) -> LifecycleTransition {
        let previous_phase = transition::reconcile_phase(self.phase, snapshot);
        let (planned_next_phase, planned_action) = transition::plan(previous_phase, input);
        let recovers_command_finished = matches!(
            (input, planned_action),
            (
                LifecycleInput::CommandFinished(NextBlockIdDisposition::Novel),
                LifecycleAction::AcceptCommandFinished,
            )
        ) && match previous_phase {
            LifecyclePhase::AwaitingPrecmd | LifecyclePhase::Unknown => true,
            LifecyclePhase::AtPrompt => snapshot.is_bootstrap_done,
            LifecyclePhase::Submitted | LifecyclePhase::Executing | LifecyclePhase::Terminated => {
                false
            }
        };
        let is_gated_recovery = recovers_command_finished
            || matches!(
                planned_action,
                LifecycleAction::RefreshPrecmd
                    | LifecycleAction::ReconcileCompletionThenApplyPrecmd
            );
        let (next_phase, action) =
            if is_gated_recovery && !FeatureFlag::TerminalLifecycleRecovery.is_enabled() {
                (
                    previous_phase,
                    LifecycleAction::Ignore(IgnoreReason::RecoveryDisabled),
                )
            } else {
                (planned_next_phase, planned_action)
            };
        let reconciles_missing_execution = matches!(
            (input, planned_action),
            (
                LifecycleInput::CommandFinished(NextBlockIdDisposition::Novel),
                LifecycleAction::AcceptCommandFinished,
            )
        ) && !snapshot.finished
            && snapshot.block_state != BlockState::Executing;
        let should_record = action.is_ignored()
            || is_gated_recovery
            || reconciles_missing_execution
            || snapshot.completion_mismatch
            || matches!(
                (previous_phase, input),
                (
                    LifecyclePhase::AwaitingPrecmd | LifecyclePhase::Unknown,
                    LifecycleInput::StartCommand(_) | LifecycleInput::Preexec(_)
                )
            );
        let recovery_record = should_record
            .then(|| {
                LifecycleRecoveryRecord::new(
                    previous_phase,
                    next_phase,
                    input.kind(),
                    action,
                    snapshot,
                )
            })
            .and_then(|record| self.telemetry_limiter.record(record));

        LifecycleTransition {
            previous_phase,
            next_phase,
            action,
            recovery_record,
        }
    }

    pub(super) fn commit(&mut self, transition: &LifecycleTransition) {
        if matches!(transition.action, LifecycleAction::BeginEpoch) {
            self.epoch = self.epoch.wrapping_add(1);
        }
        self.phase = transition.next_phase;
    }

    pub(super) fn reset_unknown(&mut self) {
        self.phase = LifecyclePhase::Unknown;
    }
}

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;
