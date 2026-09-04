import re

path = "/opt/extra-putaway-sorting/server.py"
with open(path) as f:
    code = f.read()

# Show current _zone_label function
lines = code.split('\n')
in_func = False
for i, l in enumerate(lines):
    if 'def _zone_label' in l:
        in_func = True
    if in_func:
        print(f"{i:4}: {l}")
        if in_func and i > 0 and l.strip() == '' and i > lines.index(l):
            pass
        # Stop after we see a return and a blank line
        if in_func and l.strip().startswith('return') :
            # print a few more lines
            for j in range(i+1, min(i+4, len(lines))):
                print(f"{j:4}: {lines[j]}")
            break
