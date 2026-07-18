from dataclasses import dataclass

from transhooter_worker.domain.models import (
    ErrorKind,
    ProviderError,
    RetryAction,
    RetryAdvice,
    RetryDecision,
)


@dataclass(frozen=True, slots=True)
class FrozenRetryPolicy:
    maximum_attempts: int
    base_delay_ms: int
    maximum_delay_ms: int

    def decide(
        self,
        error: ProviderError,
        attempt_number: int,
        accepted_input: int,
        received_output: int,
        emitted_output: int,
        jitter_unit: float,
    ) -> RetryDecision:
        if not 0 <= jitter_unit <= 1:
            raise ValueError("jitter must be normalized")
        unsafe = received_output > 0 or emitted_output > 0
        if (
            error.provider_retry_advice is RetryAdvice.NEVER
            or unsafe
            or attempt_number >= self.maximum_attempts
        ):
            return RetryDecision(RetryAction.DEGRADE, None, "unsafe or exhausted", error.attempt_id)
        if (
            accepted_input
            and error.scope == "operation"
            and error.kind in {ErrorKind.PROVIDER, ErrorKind.INVALID_REQUEST}
        ):
            return RetryDecision(
                RetryAction.DEGRADE, None, "provider accepted non-replayable work", error.attempt_id
            )
        ceiling = min(self.maximum_delay_ms, self.base_delay_ms * (2 ** (attempt_number - 1)))
        provider_delay = error.retry_delay_ms or 0
        delay = max(provider_delay, int(ceiling * jitter_unit))
        return RetryDecision(RetryAction.RETRY, delay, "safe uncommitted replay", error.attempt_id)
