def renamekwargs(kwargs, names={'cls': 'class'}):
    for name, newname in names.items():
        if name in kwargs and newname not in kwargs:
            kwargs[newname] = kwargs[name]
            del kwargs[name]
    return kwargs
