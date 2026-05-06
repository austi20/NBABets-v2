from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

_JSON_INSTRUCTION = (
    "You are the local autonomy copilot for NBA prop modeling. Return STRICT JSON only with keys: "
    "status, confidence, summary, actions."
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="LoRA SFT on Qwen-class base model using TRL (optional dependency group).")
    parser.add_argument("--train-jsonl", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--base-model",
        type=str,
        default="Qwen/Qwen2.5-1.5B-Instruct",
        help="HF model id (swap to Qwen3-1.7B instruct when available on your hub).",
    )
    parser.add_argument("--max-seq-length", type=int, default=4096)
    parser.add_argument("--epochs", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--grad-accum", type=int, default=8)
    parser.add_argument("--lora-r", type=int, default=16)
    parser.add_argument("--lora-alpha", type=int, default=32)
    parser.add_argument("--learning-rate", type=float, default=2e-4)
    args = parser.parse_args(argv)

    try:
        import torch
        from datasets import Dataset
        from peft import LoraConfig
        from transformers import AutoModelForCausalLM, AutoTokenizer
        from trl import SFTConfig, SFTTrainer
    except ImportError as exc:  # pragma: no cover - optional stack
        raise SystemExit(
            "Missing optional dependencies. Install with: pip install -e '.[llm-train]'"
        ) from exc

    rows: list[dict[str, Any]] = []
    with args.train_jsonl.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    if not rows:
        raise SystemExit("train-jsonl contained no rows")

    tokenizer = AutoTokenizer.from_pretrained(args.base_model, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    def _format_example(sample: dict[str, Any]) -> dict[str, str]:
        messages = sample.get("messages") or []
        text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False)
        return {"text": text}

    dataset = Dataset.from_list(rows).map(_format_example)

    dtype = torch.bfloat16 if torch.cuda.is_available() else torch.float32
    model = AutoModelForCausalLM.from_pretrained(
        args.base_model,
        trust_remote_code=True,
        torch_dtype=dtype,
        device_map="auto" if torch.cuda.is_available() else None,
    )

    lora_config = LoraConfig(
        r=args.lora_r,
        lora_alpha=args.lora_alpha,
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
        target_modules=("q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"),
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    cfg_kwargs: dict[str, Any] = dict(
        output_dir=str(args.output_dir),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        gradient_accumulation_steps=args.grad_accum,
        learning_rate=args.learning_rate,
        logging_steps=10,
        save_strategy="epoch",
        bf16=torch.cuda.is_available(),
    )
    if hasattr(SFTConfig, "max_seq_length"):
        cfg_kwargs["max_seq_length"] = args.max_seq_length
    elif hasattr(SFTConfig, "max_length"):
        cfg_kwargs["max_length"] = args.max_seq_length
    if hasattr(SFTConfig, "dataset_text_field"):
        cfg_kwargs["dataset_text_field"] = "text"
    if hasattr(SFTConfig, "packing"):
        cfg_kwargs["packing"] = False
    sft_config = SFTConfig(**cfg_kwargs)

    trainer_kwargs: dict[str, Any] = dict(
        model=model,
        args=sft_config,
        train_dataset=dataset,
        peft_config=lora_config,
    )
    try:
        trainer = SFTTrainer(processing_class=tokenizer, **trainer_kwargs)
    except TypeError:
        trainer = SFTTrainer(tokenizer=tokenizer, **trainer_kwargs)

    trainer.train()
    adapter_dir = args.output_dir / "adapter"
    trainer.save_model(str(adapter_dir))
    tokenizer.save_pretrained(str(adapter_dir))
    (args.output_dir / "train_manifest.json").write_text(
        json.dumps(
            {
                "base_model": args.base_model,
                "train_jsonl": str(args.train_jsonl),
                "max_seq_length": args.max_seq_length,
                "epochs": args.epochs,
                "instruction_echo": _JSON_INSTRUCTION[:80],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Saved adapter to {adapter_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
