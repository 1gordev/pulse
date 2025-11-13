package com.id.pulse.modules.connector.model.enums;

/**
 * Identifies why a connector runner is being polled.
 */
public enum ConnectorCallReason {
    LIVE,
    RE_PROCESSING,
    TIME_REALIGN
}
