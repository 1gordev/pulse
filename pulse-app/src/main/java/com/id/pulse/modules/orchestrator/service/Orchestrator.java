package com.id.pulse.modules.orchestrator.service;

import com.id.pulse.modules.orchestrator.logic.ConnectionActuator;
import com.id.pulse.modules.orchestrator.logic.GroupEnableChangeDetector;
import com.id.pulse.modules.poller.service.ChannelPoller;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
public class Orchestrator {

    private final ApplicationContext appCtx;
    private final AtomicBoolean connectionActuatorRunning = new AtomicBoolean(false);
    private final AtomicBoolean groupEnableChangeDetectionRunning = new AtomicBoolean(false);
    private final AtomicBoolean pollChannelsRunning = new AtomicBoolean(false);
    private final ChannelPoller channelPoller;

    public Orchestrator(ApplicationContext appCtx, ChannelPoller channelPoller) {
        this.appCtx = appCtx;
        this.channelPoller = channelPoller;
    }


    @Scheduled(fixedDelay = 2500)
    @Async
    public void triggerGroupEnableChangeDetection() {
        if (groupEnableChangeDetectionRunning.compareAndSet(false, true)) {
            try {
                log.trace("Running group enable change detection");
                appCtx.getBean(GroupEnableChangeDetector.class).run();
            } catch (Exception ex) {
                log.error("Error during group enable change detection", ex);
            } finally {
                log.trace("Finished group enable change detection");
                groupEnableChangeDetectionRunning.set(false);
            }
        }
    }

    @Scheduled(fixedDelay = 2500)
    @Async
    public void triggerConnectionActuator() {
        if (connectionActuatorRunning.compareAndSet(false, true)) {
            try {
                log.trace("Running connection actuator");
                appCtx.getBean(ConnectionActuator.class).run();
            } catch (Exception ex) {
                log.error("Error during connection actuator run()", ex);
            } finally {
                connectionActuatorRunning.set(false);
            }
        }
    }

    @Scheduled(fixedRate = 100)
    public void pollChannels() {
        if(pollChannelsRunning.compareAndSet(false, true)) {
            try {
                channelPoller.run();
            } catch (Exception ex) {
                log.error("Error during poll channels", ex);
            } finally {
                pollChannelsRunning.set(false);
            }
        }
    }
}
