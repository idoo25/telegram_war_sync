# Tests

Run all unit tests:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

If `python` is not in PATH on Windows:

```powershell
py -3 -m unittest discover -s tests -p "test_*.py" -v
```
