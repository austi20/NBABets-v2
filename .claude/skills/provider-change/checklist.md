# Provider change checklist

- Is the provider registered in `app/providers/factory.py`?
- Are cache wrappers still applied correctly?
- Are fallback chains still explicit?
- Did the change preserve API key and tier checks?
- Is there a focused test for schema or fallback behavior?
- Did you avoid broad cross-provider abstraction changes?
