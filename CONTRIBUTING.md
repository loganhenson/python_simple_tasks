## **Testing**

### **Run Unit Tests**
Ensure PostgreSQL is running and execute all tests:
```bash
python -m unittest discover python_simple_tasks/tests
```

## Updating pip package

```bash
python -m build
twine upload dist/*
```
