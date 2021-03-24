#!/usr/bin/env python3

from pathlib import Path

kernel_text = """{
 "argv": [
  "/usgs/cpkgs/anaconda3_linux/envs/autocnet/bin/python",
  "-m",
  "ipykernel_launcher",
  "-f",
  "{connection_file}"
 ],
 "display_name": "Autocnet (workshop)",
 "language": "python"
}
"""

kernel_dir = Path.home() / '.local/share/jupyter/kernels/autocnet_workshop/'
kernel_dir.mkdir(parents=True, exist_ok=True)
kernel_file = kernel_dir / 'kernel.json'

kernel_file.open('w').write(kernel_text)
