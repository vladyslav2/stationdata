new_hook = open('./pre-commit').read()

f = open('../../../../.git/hooks/pre-commit', 'r+')
old_content = f.read()
new_content = old_content.replace('#!/bin/sh', new_hook)

f.seek(0)
f.write(new_content)