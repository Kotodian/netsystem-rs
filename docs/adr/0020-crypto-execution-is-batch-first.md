# Cryptographic execution is batch-first

All primitive cryptographic execution crosses the Crypto Engine as a typed Crypto Batch, including singleton work represented as a one-element batch. This preserves VPP-style batching for multi-buffer, SIMD, and hardware implementations while using separate typed operation families instead of one flag-driven nullable record; contiguous and scatter-gather input plus in-place and out-of-place output are part of the initial contract.
