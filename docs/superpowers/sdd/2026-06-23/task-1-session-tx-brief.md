## Task 1: 把 app/session copy 边界前移到 session-owned TX chain

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-service/tests/session_runtime.rs`

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/session_runtime.rs` 增加：

```rust
#[test]
fn session_app_send_is_copied_into_session_owned_tx_chain_before_transport() {
    let (mut driver, ring, session_id) = test_session_driver();
    let send = ring_send(&ring, b"hello world");

    driver.app_mut().push_pending_send(session_id, send);
    driver.poll_app().expect("poll app");

    assert!(driver.app().pending_send_len(session_id).expect("pending len").is_none());
    assert!(driver.has_retained_tx(session_id));
}

#[test]
fn session_tx_flush_uses_retained_chain_without_recopying_from_app_ring() {
    let (runtime, mut driver, ring, session_id) = test_session_flush_driver();
    let send = ring_send(&ring, b"abcdef");

    driver.app_mut().push_pending_send(session_id, send);
    driver.poll_app().expect("poll app");
    dispatch_session_queue_once(&runtime, &mut driver).expect("dispatch");

    assert!(driver.has_retained_tx(session_id));
    assert!(driver.app().pending_send_len(session_id).expect("pending len").is_none());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test session_runtime session_app_send_is_copied_into_session_owned_tx_chain_before_transport -- --exact`

Expected: FAIL，因为当前 `poll_app()` 之后 `pending_send_len()` 仍然依赖 `AppSendData`，并且还没有 retained TX chain。

- [ ] **Step 3: 实现最小改造**

在 `crates/hammer-service/src/session/app.rs` 改成以下思路：

```rust
impl SessionAppRuntime {
    pub(crate) fn push_pending_send(
        &mut self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        send: AppSendData,
    ) -> CoreResult<()> {
        let total_len = send.len()?;
        let head = copy_send_into_session_chain(buffers, &send)?;
        send.release();
        let progress = SessionAppTxProgress {
            head,
            sent_offset: 0,
            total_len,
        };
        // existing queue insertion
        Ok(())
    }
}

fn copy_send_into_session_chain(
    buffers: &DataPlaneBuffers,
    send: &AppSendData,
) -> CoreResult<BufferIndex> {
    let mut remaining = send.len()?;
    let mut copied = 0usize;
    let head = buffers.alloc_index()?;
    let mut current = head;
    while remaining != 0 {
        let writable = buffers.get_buffer_mut(current)?.writable_tail_mut().len();
        let chunk = writable.min(remaining);
        let buffer = &mut buffers.get_buffer_mut(current)?;
        let written = send.copy_to(copied, &mut buffer.writable_tail_mut()[..chunk]).map_err(CoreError::from)?;
        buffer.commit_writable_tail(written)?;
        copied += written;
        remaining -= written;
        if remaining != 0 {
            let next = buffers.alloc_index()?;
            buffers.append_existing_chain(current, next)?;
            current = next;
        }
    }
    Ok(head)
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service --test session_runtime session_tx_flush_uses_retained_chain_without_recopying_from_app_ring -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/app.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "session(Refactor): move app send ownership into session tx chains"
```

