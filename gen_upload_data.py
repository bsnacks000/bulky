import csv

with open("fake-upload.csv", "w") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "val_a",
            "val_b",
            "val_c",
            "val_d",
            "val_e",
            "val_f",
            "val_g",
            "val_h",
        ],
    )
    writer.writeheader()

    for i in range(10000):
        writer.writerow(
            {
                "val_a": i * 1.0,
                "val_b": i * 2.0,
                "val_c": i * 3.0,
                "val_d": i * 4.0,
                "val_e": i * 5.0,
                "val_f": i * 6.0,
                "val_g": i * 7.0,
                "val_h": i * 8.0,
            }
        )
