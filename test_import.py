import sys
print("Python path:", sys.executable)
print("Python version:", sys.version)

try:
    from quixstreams import Application
    print("SUCCESS: quixstreams imported successfully!")
except ImportError as e:
    print(f"IMPORT ERROR: {e}")