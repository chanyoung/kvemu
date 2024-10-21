# AD/AE n.187

This source code is based on [FEMU](https://github.com/MoatSysLab/FEMU).

Installation
------------

1. Make sure you have installed necessary libraries for building QEMU. The
   dependencies can be installed by following instructions below:

```bash
  cd ~
  git clone https://github.com/chanyoung/kvemu.git
  cd kvemu
  mkdir build-femu
  # Switch to the FEMU building directory
  cd build-femu
  # Copy femu script
  cp ../femu-scripts/femu-copy-scripts.sh .
  ./femu-copy-scripts.sh .
  # only Debian/Ubuntu based distributions supported
  sudo ./pkgdep.sh
```

2. Compile & Install FEMU:

```bash
  ./femu-compile.sh
```
  FEMU binary will appear as ``x86_64-softmmu/qemu-system-x86_64``

3. Prepare the VM image

  [Download link](https://drive.google.com/file/d/1DJfaHnQpUn0pv0Tk2maNcmnwE8CFuKzN/view?usp=drive_link)

```
    $ mkdir -p ~/images/
    $ cd ~/images
    $ pip install gdown
    $ gdown --id 1DJfaHnQpUn0pv0Tk2maNcmnwE8CFuKzN
    $ gzip -d asplos.ubuntu.qcow2.gz
```

  - After guest OS is installed, boot it with
```
    $ cd ~/kvemu/build-femu
    $ run-pink.sh
    # Or
    $ run-lksv3.sh
```

