Task 1: user approved generic buffer attach_clone/refcount primitive in hammer-adapter::buffer; no wrapper owner type, release stays on free_index().
Task 1: complete (working-tree diff approved by reviewer)
User note: clean target after final verification.
Task 2: blocked on Task 3 TX-path ownership boundary; controller merged Task 2+3 execution to avoid adding new buffer sub-range API.
