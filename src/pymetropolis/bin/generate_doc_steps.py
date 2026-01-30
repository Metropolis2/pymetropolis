from pymetropolis.schema import STEPS

if __name__ == "__main__":
    all_params = dict()
    steps_doc = "# Steps\n\n"

    for step in sorted(STEPS, key=lambda s: s.__name__):
        steps_doc += step._md_doc()
        steps_doc += "\n"
        for _, param_obj in step._iter_params():
            key_str = ".".join(param_obj.key)
            all_params[key_str] = param_obj
    # print(steps_doc)

    params_doc = "# Parameters\n\n"
    # Iterate over the parameters, considering first the single-key parameters, then the table,
    #  parameters, sorted by alphabetical order.
    for param_obj in sorted(all_params.values(), key=lambda p: (len(p.key) != 1, p.key)):
        params_doc += param_obj._md_doc()
        params_doc += "\n"
    print(params_doc)
