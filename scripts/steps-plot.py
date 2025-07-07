import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import matplotlib.pyplot as plt
    import os
    from collections import defaultdict
    return defaultdict, os, plt


@app.cell
def _(os):
    def get_data(lines: list[str]):
        data = []
        for line in lines:
            line = line.strip()
            parts = line.split(",")
            if len(parts) == 1:
                continue

            kvs = [p.split(":") for p in parts]
            try:
                data.append({p[0].strip(): int(p[1].strip()) for p in kvs})
            except IndexError:
                print(kvs)
        return data

    files = [file for file in os.listdir("./logs") if file.startswith("steps-")]
    files = sorted(files)
    filename = f"./logs/{files.pop()}"

    print(filename)
    with open(filename) as f:
        lines = f.readlines()

    data = get_data(lines)
    return (data,)


@app.cell
def _(data, defaultdict):
    timings = defaultdict(list)
    for timing in data:
        for k, v in timing.items():
            timings[k].append(v)
    return (timings,)


@app.cell
def _(timings):
    timings.keys()
    return


@app.cell
def _(plt, timings):
    ax, fig = plt.subplots()
    fig.plot(timings["SkipList_InsertKey"][:100_0])
    fig.set_yscale("log")
    ax
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
