#!/bin/sh

# network:
# virtual network bridge (virbr0) can be created by libvirt
# maybe a static IP address is needed in vm

# disk:
# truncate -s 32G zns.raw
# dd if=/dev/zero of=zns.raw bs=1M count=32768

# from https://zonedstorage.io/docs/getting-started/zbd-emulation#qemu-execution-example

#
# Some variables
#
bridge="virbr0"
vmimg="aquafs-ubuntu.qcow2"
znsimg="zns.raw"

#
# Run QEMU
#
qemu-system-x86_64 \
-name guest=UbuntuServer \
-machine pc-q35-5.2,accel=kvm \
-m 16G \
-smp 16 \
-rtc base=utc,driftfix=slew \
-nographic \
-no-hpet \
-global ICH9-LPC.disable_s3=1 \
-global ICH9-LPC.disable_s4=1 \
-boot strict=on \
-audiodev none,id=noaudio \
-object rng-random,id=objrng0,filename=/dev/urandom \
-msg timestamp=on \
-device pcie-root-port,port=0x10,chassis=1,id=pci.1,bus=pcie.0,multifunction=on,addr=0x2 \
-netdev bridge,id=hostnet0,br=${bridge} \
-device virtio-net-pci,netdev=hostnet0,id=net0,mac=52:54:00:fa:2d:b9,bus=pci.1,addr=0x0 \
-device pcie-root-port,port=0x11,chassis=2,id=pci.2,bus=pcie.0,addr=0x2.0x1 \
-blockdev node-name="vmstorage",driver=qcow2,file.driver=file,file.filename="${vmimg}",file.node-name="vmstorage.qcow2",file.discard=unmap \
-device virtio-blk-pci,bus=pci.2,addr=0x0,drive="vmstorage",id=virtio-disk0,bootindex=1 \
-device pcie-root-port,port=0x12,chassis=3,id=pci.3,bus=pcie.0,addr=0x2.0x2 \
-device virtio-balloon-pci,id=balloon0,bus=pci.3,addr=0x0 \
-device pcie-root-port,port=0x13,chassis=4,id=pci.4,bus=pcie.0,addr=0x2.0x3 \
-device virtio-rng-pci,rng=objrng0,id=rng0,bus=pci.4,addr=0x0 \
-device pcie-root-port,port=0x14,chassis=5,id=pci.5,bus=pcie.0,addr=0x2.0x4 \
-device nvme,id=nvme0,serial=deadbeef,zoned.zasl=5,bus=pci.5 \
-drive file=${znsimg},id=nvmezns0,format=raw,if=none \
-device nvme-ns,drive=nvmezns0,bus=nvme0,nsid=1,logical_block_size=4096,physical_block_size=4096,zoned=true,zoned.zone_size=64M,zoned.zone_capacity=62M,zoned.max_open=16,zoned.max_active=32,uuid=5e40ec5f-eeb6-4317-bc5e-c919796a5f79