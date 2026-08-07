from pathlib import Path
p = Path('scripts/check_rsi_nulls.py')
code = p.read_text()
exec(code)
