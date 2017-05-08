package org.cisiondata.modules.jstorm.spout.kafka;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface FailedMsgRetryManager extends Serializable {

    /**
     * Initialization
     */
    @SuppressWarnings("rawtypes")
	void prepare(SpoutConfig spoutConfig, Map stormConf);

    /**
     * Message corresponding to the offset failed in kafka spout.
     * @param offset
     */
    void failed(Long offset);

    /**
     * Message corresponding to the offset, was acked to kafka spout.
     * @param offset
     */
    void acked(Long offset);

    /**
     * Message corresponding to the offset, has been re-emitted and under transit.
     * @param offset
     */
    void retryStarted(Long offset);

    /**
     * The offset of message, which is to be re-emitted. Spout will fetch messages starting from this offset
     * and resend them, except completed messages.
     * @return
     */
    Long nextFailedMessageToRetry();

    /**
     * @param offset
     * @return True if the message corresponding to the offset should be emitted NOW. False otherwise.
     */
    boolean shouldReEmitMsg(Long offset);

    /**
     * Spout will clean up the state for this offset if false is returned.
     * @param offset
     * @return True if the message will be retried again. False otherwise.
     */
    boolean retryFurther(Long offset);

    /**
     * Spout will call this method after retryFurther returns false.
     * This gives a chance for hooking up custom logic before all clean up.
     * @param partition,offset
     */
    void cleanOffsetAfterRetries(Partition partition, Long offset);

    /**
     * Clear any offsets before kafkaOffset. These offsets are no longer available in kafka.
     * @param kafkaOffset
     * @return Set of offsets removed.
     */
    Set<Long> clearOffsetsBefore(Long kafkaOffset);
}
