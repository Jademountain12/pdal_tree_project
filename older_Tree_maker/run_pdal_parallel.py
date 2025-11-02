import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
import time
start = time.time()
# run your executor.map(...)
print(f"Took {round(time.time() - start, 1)} seconds")


input_folder = r"./input/"
pipeline_json = r"./script/.json"
output_folder = r"./output/"
max_workers = 22  # Adjust for your CPU

def run_pipeline(laz_path):
    filename_no_ext = os.path.splitext(os.path.basename(laz_path))[0]

    # Build command with overridden inputs and outputs
    cmd = [
        "pdal", "pipeline", pipeline_json,
        f"--readers.las.filename={laz_path}",
        f"--stage.water.filename={output_folder}/{filename_no_ext}_water_class.tif",
        f"--stage.sub15.filename={output_folder}/{filename_no_ext}_class15.tif",
        f"--stage.sub25.filename={output_folder}/{filename_no_ext}_class25.tif",
        f"--stage.r075_130.filename={output_folder}/{filename_no_ext}_range075_130.tif",
        f"--stage.h130_300.filename={output_folder}/{filename_no_ext}_range130_300.tif",
        f"--stage.r300_1500.filename={output_folder}/{filename_no_ext}_range300_1500.tif",
        f"--stage.above1500.filename={output_folder}/{filename_no_ext}_above15m.tif",
        f"--stage.hagmax.filename={output_folder}/{filename_no_ext}_hag_max.tif"
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"✅ Finished: {filename_no_ext}")
    except subprocess.CalledProcessError:
        print(f"❌ Failed: {filename_no_ext}")

if __name__ == "__main__":
    laz_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".copc.laz")]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        executor.map(run_pipeline, laz_files)
