#!/usr/bin/env python3
"""Process entrypoint; runtime.py owns lifecycle and handoff orchestration."""
import asyncio
import runtime

if __name__ == "__main__":
    asyncio.run(runtime.main())
