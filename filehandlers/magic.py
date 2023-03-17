import magic
mime = magic.Magic(mime=True)
t = mime.from_file("foo.csv")
print(t)