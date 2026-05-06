import * as ToggleGroup from "@radix-ui/react-toggle-group";

export type FilterOption = {
  value: string;
  label: string;
};

type FilterStripProps = {
  label: string;
  options: FilterOption[];
  value: string;
  onValueChange: (value: string) => void;
};

export function FilterStrip({ label, options, value, onValueChange }: FilterStripProps) {
  return (
    <div className="filter-strip">
      <span className="micro-label">{label}</span>
      <ToggleGroup.Root
        type="single"
        value={value}
        onValueChange={(next) => {
          if (next) {
            onValueChange(next);
          }
        }}
        className="filter-group"
        aria-label={`${label} filters`}
      >
        {options.map((option) => (
          <ToggleGroup.Item key={option.value} value={option.value} className="filter-pill" aria-label={option.label}>
            {option.label}
          </ToggleGroup.Item>
        ))}
      </ToggleGroup.Root>
    </div>
  );
}
