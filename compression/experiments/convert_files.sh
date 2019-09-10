
# mv FA391677-4E3D-C648-8CFE-2F6DF82A6795.root doublemu_lzma.root
# hadd -f101 doublemu_zlib.root doublemu_lzma.root &
# hadd -f404 doublemu_lz4.root doublemu_lzma.root &
# hadd -f0 doublemu_uncomp.root doublemu_lzma.root &

# hadd -O -f101 doublemu_zlib_reopt.root doublemu_lzma.root &
# hadd -O -f404 doublemu_lz4_reopt.root doublemu_lzma.root &
# hadd -O -f0 doublemu_uncomp_reopt.root doublemu_lzma.root &

# hadd -O -f401 doublemu_lz4_1.root doublemu_lzma.root &
# hadd -O -f402 doublemu_lz4_2.root doublemu_lzma.root
# hadd -O -f403 doublemu_lz4_3.root doublemu_lzma.root &
# hadd -O -f404 doublemu_lz4_4.root doublemu_lzma.root
# hadd -O -f405 doublemu_lz4_5.root doublemu_lzma.root &
# hadd -O -f406 doublemu_lz4_6.root doublemu_lzma.root
# hadd -O -f407 doublemu_lz4_7.root doublemu_lzma.root &
hadd -O -f408 doublemu_lz4_8.root doublemu_lzma.root
# hadd -O -f409 doublemu_lz4_9.root doublemu_lzma.root &
