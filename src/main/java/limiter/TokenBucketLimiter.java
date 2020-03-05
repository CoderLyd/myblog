package limiter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TokenBucketLimiter implements RateLimiter {
    private final long startTimeNanos = System.nanoTime();
    private long maxTokenNum;
    private long tokenGenPeriodMicros;
    private BucketMode bucketMode;
    private final AtomicReference<TokenBucketLimiter.BucketConfig> bucketConfig;

    public static void main(String[] args) throws Exception {
        TokenBucketLimiter limiter = TokenBucketLimiter.builder()
                .withBucketMode(BucketMode.BTB)
                .withTokenPerSecond(1000)
                .build();
        for (int i = 0; i < 100; i++) {
            limiter.getToken();
            MICROSECONDS.sleep(100);
        }
    }

    private TokenBucketLimiter(long tokenGenPeriod, long maxTokenNum, BucketMode bucketMode, AtomicReference<TokenBucketLimiter.BucketConfig> bucketConfig) {
        this.tokenGenPeriodMicros = tokenGenPeriod;
        this.maxTokenNum = maxTokenNum;
        this.bucketConfig = bucketConfig;
        this.bucketMode = bucketMode;
    }

    @Override
    public boolean getToken() {
        switch (bucketMode) {
            case FFTB:
                return getTokenInFFTBMode();
            case NTB:
                return getTokenInNTBMode();
            default:
                return getTokenInBTBMode();
        }

    }

    private boolean getTokenInBTBMode() {
        BucketConfig current;
        BucketConfig next;
        long nowMicros = duration();
        synchronized (this) {
            while (true) {
                current = bucketConfig.get();
                long curBuckets = current.curBuckets;
                if (nowMicros > current.preTokenGenMicros) {
                    long newTokens = (nowMicros - current.preTokenGenMicros) / tokenGenPeriodMicros;
                    curBuckets = Math.min(maxTokenNum, current.curBuckets + newTokens);
                }
                if (curBuckets > 1) {
                    curBuckets--;
                    next = new BucketConfig(curBuckets, nowMicros, curBuckets >= 1);
                    bucketConfig.set(next);
                    return true;
                } else {
                    uninterruptibleSleep(tokenGenPeriodMicros, MICROSECONDS);
                }
            }

        }
    }

    private boolean getTokenInFFTBMode() {
        BucketConfig current;
        BucketConfig next;
        long nowMicros = duration();
        synchronized (this) {
            current = bucketConfig.get();
            long curBuckets = current.curBuckets;
            boolean permitted = current.permitted;
            if (nowMicros > current.preTokenGenMicros) {
                long newTokens = (nowMicros - current.preTokenGenMicros) / tokenGenPeriodMicros;
                curBuckets = Math.min(maxTokenNum, current.curBuckets + newTokens);
            }
            if (curBuckets > 0) {
                --curBuckets;
                permitted = true;
            } else {
                permitted = false;
            }
            next = new BucketConfig(curBuckets, nowMicros, permitted);
            bucketConfig.set(next);
            return permitted;
        }
    }

    private boolean getTokenInNTBMode() {
        BucketConfig current;
        BucketConfig next;
        boolean permitted;
        do {
            current = this.bucketConfig.get();
            long curMicros = this.duration();
            long curBuckets = current.curBuckets;
            long preTokenGenMicros = current.preTokenGenMicros;
            permitted = current.permitted;
            if (curMicros > preTokenGenMicros) {
                long newTokens = (curMicros - preTokenGenMicros) / this.tokenGenPeriodMicros;
                curBuckets = Math.min(this.maxTokenNum, curBuckets + newTokens);
                preTokenGenMicros = curMicros;
            } else {
                break;
            }

            if (curBuckets >= 1) {
                --curBuckets;
                permitted = true;
            } else {
                permitted = false;
            }
            next = new BucketConfig(curBuckets, preTokenGenMicros, permitted);
        } while (!this.compareAndSet(current, next));
        return permitted;
    }

    private long duration() {
        return TimeUnit.MICROSECONDS.convert(System.nanoTime() - this.startTimeNanos, TimeUnit.NANOSECONDS);
    }

    private boolean compareAndSet(BucketConfig current, BucketConfig next) {
        if (this.bucketConfig.compareAndSet(current, next)) {
            return true;
        } else {
            LockSupport.parkNanos(1L);
            return false;
        }
    }

    private void uninterruptibleSleep(long sleepTime, TimeUnit unit) {
        boolean interrupted = false;
        try {
            long remainingNanos = unit.toNanos(sleepTime);
            long end = System.nanoTime() + remainingNanos;
            while (true) {
                try {
                    NANOSECONDS.sleep(remainingNanos);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static BucketBuilder builder() {
        return new BucketBuilder();
    }

    public static class BucketBuilder {
        private int tokenPerSecond;
        private long maxTokenNum;
        private BucketMode bucketMode = BucketMode.BTB;

        public BucketBuilder withTokenPerSecond(int tokenPerSecond) {
            if (tokenPerSecond <= 0) {
                throw new IllegalArgumentException("tokenPerSecond must >0");
            }
            this.tokenPerSecond = tokenPerSecond;
            return this;
        }

        public BucketBuilder withMaxTokenNum(long maxTokenNum) {
            this.maxTokenNum = maxTokenNum;
            return this;
        }

        public BucketBuilder withBucketMode(BucketMode bucketMode) {
            this.bucketMode = bucketMode;
            return this;
        }

        public TokenBucketLimiter build() {
            AtomicReference<BucketConfig> bucketConfigRef = new AtomicReference<>(new BucketConfig(0, 0, true));
            long tokenGenPeriod = TimeUnit.SECONDS.toMicros(1) / tokenPerSecond;
            return new TokenBucketLimiter(tokenGenPeriod, tokenPerSecond, bucketMode, bucketConfigRef);
        }


    }

    public static enum BucketMode {

        /**
         * blocked token bucket
         */
        BTB("btb"),


        /**
         * none blocked token bucket
         */
        NTB("ntb"),


        /**
         * 没有可用token 抛出异常的token bucket
         */
        FFTB("fftb");

        private String name;

        BucketMode(String name) {
            this.name = name;
        }

    }

    private static class BucketConfig {
        private final long curBuckets;
        private final long preTokenGenMicros;
        private final boolean permitted;

        private BucketConfig(long curBuckets, long preTokenGenMicros, boolean permitted) {
            this.curBuckets = curBuckets;
            this.preTokenGenMicros = preTokenGenMicros;
            this.permitted = permitted;
        }
    }
}
