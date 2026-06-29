import os

def generate_tree(start_dir):
    for root, dirs, files in os.walk(start_dir):
        # Skip virtual environments or git directories to keep it clean
        dirs[:] = [d for d in dirs if d not in ('.venv', '.git', '__pycache__')]
        
        level = root.replace(start_dir, '').count(os.sep)
        indent = '│   ' * (level)
        print(f"{indent}├── {os.path.basename(root)}/")
        
        sub_indent = '│   ' * (level + 1)
        for f in files:
            print(f"{sub_indent}├── {f}")

# Execute from current working directory
generate_tree(os.getcwd())