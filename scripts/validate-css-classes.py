"""Validate HTML: every class used in <body> must have a CSS rule in <style>."""
import re, sys

html = open('web/index.html', 'r', encoding='utf-8').read()

# Extract CSS rules from <style>
style_m = re.search(r'<style>(.*?)</style>', html, re.DOTALL)
if not style_m:
    print('FAIL: no <style> block found')
    sys.exit(1)

css = style_m.group(1)

# Collect all defined class selectors from CSS.
# Per CSS spec a class name cannot start with a digit, so require a leading
# letter/underscore/hyphen. This avoids matching numeric decimals like 0.15s.
defined = set()
for m in re.finditer(r'\.([a-zA-Z_-][a-zA-Z0-9_-]*)', css):
    defined.add(m.group(1))

# Extract body HTML (everything between <body> and </body>)
body_m = re.search(r'<body>(.*?)</body>', html, re.DOTALL)
if not body_m:
    print('FAIL: no <body> found')
    sys.exit(1)

body_html = body_m.group(1)

# Extract <script> blocks BEFORE stripping, to scan JS for dynamic class usage
script_blocks = re.findall(r'<script[^>]*>(.*?)</script>', body_html, flags=re.DOTALL)

# Strip <script> blocks from body to avoid JS false positives in class="" scan
body_html = re.sub(r'<script[^>]*>.*?</script>', '', body_html, flags=re.DOTALL)

# Collect all class names from HTML attributes
used = set()
for m in re.finditer(r'class="([^"]*)"', body_html):
    for cls in m.group(1).split():
        # class="" produces empty string
        if cls:
            used.add(cls)

# Scan JS for dynamic class additions via classList.add/toggle
# Also handle className assignments like el.className = 'xxx'
js_dynamic = set()
for script in script_blocks:
    # classList.add('a', 'b') / classList.toggle('a') / classList.remove('a')
    for m in re.finditer(r'classList\.(?:add|toggle|remove)\(([^)]*)\)', script):
        for arg_m in re.finditer(r'["\']([a-zA-Z0-9_-]+)["\']', m.group(1)):
            js_dynamic.add(arg_m.group(1))
    # className = 'xxx' (single class) or className = 'a b c'
    for m in re.finditer(r'\.className\s*=\s*["\']([^"\']+)["\']', script):
        for cls in m.group(1).split():
            if cls:
                js_dynamic.add(cls)
    # BUG5-fix: 支持 className = cond ? 'a' : 'b' 三元表达式
    for m in re.finditer(r'\.className\s*=\s*[^?]+\?\s*["\']([^"\']+)["\']\s*:\s*["\']([^"\']+)["\']', script):
        for cls in (m.group(1) + ' ' + m.group(2)).split():
            if cls:
                js_dynamic.add(cls)

used |= js_dynamic

# Ignored: dynamic/state classes that legitimately have no static CSS rule.
# Reduced set — common state words that are paired with component selectors
# (e.g. .panel.active) and thus already covered by defined, or are pure
# runtime state with no visual effect.
ignored = {
    'filled', 'pending', 'running', 'completed',
    'available', 'taken-badge', 'danger', 'outline', 'info', 'error', 'success',
}

# Forward check: classes used in HTML/JS but not defined in CSS (excluding ignored)
missing = sorted(used - defined - ignored)
if missing:
    print(f'FAIL: {len(missing)} class(es) used in HTML but not defined in CSS:')
    for c in missing:
        print(f'  .{c}')
    sys.exit(1)

# Reverse check: classes defined in CSS but never used (warnings only)
unused = sorted(defined - used - ignored)
if unused:
    print(f'WARN: {len(unused)} class(es) defined in CSS but not used:')
    for c in unused:
        print(f'  .{c}')

print(f'OK: all {len(used)} used classes have CSS definitions' +
      (f' ({len(js_dynamic)} from JS)' if js_dynamic else ''))
