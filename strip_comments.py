import os

def strip_comments_from_java(code):
    result = []
    in_string = False
    in_char = False
    in_line_comment = False
    in_block_comment = False
    escaped = False
    
    i = 0
    n = len(code)
    while i < n:
        c = code[i]
        
        if in_line_comment:
            if c == '\n':
                in_line_comment = False
                result.append(c)
            i += 1
            continue
            
        if in_block_comment:
            if c == '*' and i + 1 < n and code[i+1] == '/':
                in_block_comment = False
                i += 2
            else:
                if c == '\n':
                    result.append(c) # Preserve newlines to keep line numbering intact
                i += 1
            continue
            
        if in_string:
            result.append(c)
            if c == '\\' and not escaped:
                escaped = True
            elif c == '"' and not escaped:
                in_string = False
            else:
                escaped = False
            i += 1
            continue
            
        if in_char:
            result.append(c)
            if c == '\\' and not escaped:
                escaped = True
            elif c == '\'' and not escaped:
                in_char = False
            else:
                escaped = False
            i += 1
            continue
            
        # Check for start of comments
        if c == '/' and i + 1 < n:
            next_c = code[i+1]
            if next_c == '/':
                in_line_comment = True
                i += 2
                continue
            elif next_c == '*':
                in_block_comment = True
                i += 2
                continue
                
        # Check for start of string/char literal
        if c == '"':
            in_string = True
            escaped = False
        elif c == '\'':
            in_char = True
            escaped = False
            
        result.append(c)
        i += 1
        
    return "".join(result)

def process_directory(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".java"):
                filepath = os.path.join(root, file)
                print(f"Processing: {filepath}")
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                stripped = strip_comments_from_java(content)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(stripped)

if __name__ == "__main__":
    process_directory("src")
    process_directory("fuzz")
    print("All comments stripped successfully!")
