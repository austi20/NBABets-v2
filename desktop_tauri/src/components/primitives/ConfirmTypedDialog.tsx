import { useState } from "react";

interface Props {
  open: boolean;
  phrase: string;
  onConfirm: () => void;
  onCancel: () => void;
  body: string;
}

export function ConfirmTypedDialog({ open, phrase, onConfirm, onCancel, body }: Props) {
  const [v, setV] = useState("");
  if (!open) return null;
  return (
    <div role="dialog" aria-modal={true}>
      <p>{body}</p>
      <input
        placeholder={phrase}
        value={v}
        onChange={(e) => setV(e.target.value)}
        aria-label={`Type ${phrase} to confirm`}
      />
      <button type="button" onClick={onCancel}>
        Cancel
      </button>
      <button
        type="button"
        disabled={v !== phrase}
        onClick={() => {
          if (v === phrase) onConfirm();
        }}
      >
        Confirm
      </button>
    </div>
  );
}
