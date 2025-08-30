# Chunking

使用定长切块和变长切块

zxiao@localhost:/workspace/X/XlatorS$ go run utils/calcFP.go -file /tmp/1M.data
Using FixedCDC chunking algorithm.
Successfully chunked file into 8 chunks, total size: 1048576 bytes
Chunk 1: len=131072, fp=ac2dc23ccdf249a5a10fef5103d06a32abd26ade23cd3c7c8a1b0d0d6334870a
Chunk 2: len=131072, fp=1ed959c36cc04ca6139a095e92ce894c486b23cfbf36e0c880f02b6ef864b577
Chunk 3: len=131072, fp=4bbbc112dfc86e36f6a49826e7d3199749dcd4220ea573195e9ce601e3c928af
Chunk 4: len=131072, fp=e6dab58e615b960488d3f0b68395a58f68a4c12392fc4a4fce84fdad711c7696
Chunk 5: len=131072, fp=c65fc922bef345e963d7772ab422ded892cc5f58e7e991e02eb9e44fd8e372d9
Chunk 6: len=131072, fp=b8fc477b2565fcbf932df3511a193174574765dc78e2c2d68079736cebe85188
Chunk 7: len=131072, fp=14be7e6621193272d6dede799fdaa0ff4a02d475e54c9d8e0a4f68b80042adc7
Chunk 8: len=131072, fp=399d6fb092c6126bd34a14c07faf388da0c69733b27678d25a913949d3049eb5
zxiao@localhost:/workspace/X/XlatorS$ go run utils/calcFP.go -file /tmp/1M.data -fastcdc
Using FastCDC chunking algorithm.
Successfully chunked file into 6 chunks, total size: 1048576 bytes
Chunk 1: len=147231, fp=d2166aecc6fea5522707788c5552ba6c7a165547a8d537a4f84edde95299ad25
Chunk 2: len=217880, fp=7261e586326cd189cd7d321c503c2ccde0568d6e02df7196fb75735d3c575c26
Chunk 3: len=160519, fp=eeed2b64dbb418dad8c66f71bab4e6055c3302d98ac2e57ebd12466b01b1d906
Chunk 4: len=165384, fp=ee2c4ec806fa68b1310fd7c69af5579c361b252992f3edbc1dce774c6c9cfeb3
Chunk 5: len=162876, fp=c18cb98efd04bb82ff3deed7ffed92cb9ce1f5664e4e4b5e7bc4987d0e21f23e
Chunk 6: len=194686, fp=9c022fabf2d24465d03dba3cbe6d45eaa2589b8c1ad3885c7ee6203cff17aea5
zxiao@localhost:/workspace/X/XlatorS$
zxiao@localhost:/workspace/X/XlatorS$ go run utils/calcFP.go -file /tmp/1M.data -fastcdc
Using FastCDC chunking algorithm.
Successfully chunked file into 6 chunks, total size: 1048576 bytes
Chunk 1: len=147231, fp=d2166aecc6fea5522707788c5552ba6c7a165547a8d537a4f84edde95299ad25
Chunk 2: len=217880, fp=7261e586326cd189cd7d321c503c2ccde0568d6e02df7196fb75735d3c575c26
Chunk 3: len=160519, fp=eeed2b64dbb418dad8c66f71bab4e6055c3302d98ac2e57ebd12466b01b1d906
Chunk 4: len=165384, fp=ee2c4ec806fa68b1310fd7c69af5579c361b252992f3edbc1dce774c6c9cfeb3
Chunk 5: len=162876, fp=c18cb98efd04bb82ff3deed7ffed92cb9ce1f5664e4e4b5e7bc4987d0e21f23e
Chunk 6: len=194686, fp=9c022fabf2d24465d03dba3cbe6d45eaa2589b8c1ad3885c7ee6203cff17aea5
zxiao@localhost:/workspace/X/XlatorS$
