# GGUF export and safe rollout (local Qwen / llama.cpp)

The NBA app uses **llama.cpp** HTTP (`AI_LOCAL_ENDPOINT`) and a **GGUF** file (`AI_LOCAL_MODEL_PATH`). Fine-tuning happens in Hugging Face + LoRA space; deployment requires **merge → convert → swap**.

## 1. Train LoRA

```bash
pip install -e ".[llm-train]"
python -m llm_train.train.sft_lora \
  --train-jsonl llm_train/outputs/dataset/train.jsonl \
  --output-dir llm_train/outputs/run1 \
  --base-model Qwen/Qwen2.5-1.5B-Instruct
```

Swap `--base-model` to your Qwen3-1.7B instruct checkpoint when it matches the tokenizer template you used in the dataset.

## 2. Merge adapter into full weights

```bash
python -m llm_train.train.merge_adapter \
  --base-model Qwen/Qwen2.5-1.5B-Instruct \
  --adapter-dir llm_train/outputs/run1/adapter \
  --out-dir llm_train/outputs/run1/merged_hf
```

## 3. Convert HF → GGUF

Use the same `llama.cpp` convert scripts as your hybrid runtime (often under `%LOCALAPPDATA%\ClaudeHybridQwen35` or your local `llama.cpp` checkout). Typical flow:

1. `python convert_hf_to_gguf.py llm_train/outputs/run1/merged_hf --outfile qwen-accuracy-YYYYMMDD.gguf --outtype q8_0`
2. Validate with `llama-cli` or your existing `llama-server` smoke test.

Exact flags depend on your `llama.cpp` revision; follow upstream docs for Qwen/Llama architecture mapping.

## 4. Roll out without clobbering the known-good model

1. Copy the new file next to the current GGUF, e.g. `qwen3-1.7b-accuracy-20260414.gguf`.
2. Update `.env`:
   - `AI_LOCAL_MODEL_PATH=E:\path\to\qwen3-1.7b-accuracy-20260414.gguf`
   - Optionally bump `AI_LOCAL_MODEL` alias string for logs (`qwen3-1.7b-q8-accuracy`).
3. Restart **llama-server** (or your `claude_qwen35_hybrid.ps1 start-app-runtime` flow).
4. Smoke test: hit `/health`, then run a short `model_health` prompt via the desktop or `pytest` integration if you add one.

## 5. Roll back

Point `AI_LOCAL_MODEL_PATH` back to the previous GGUF and restart the server.

## Notes

- **Do not** delete the last known-good GGUF until the new one passes a full startup cycle.
- Quantization (`q8_0`, etc.) should match what your GPU/CPU budget allows; training is usually BF16/FP16, export is separate.
