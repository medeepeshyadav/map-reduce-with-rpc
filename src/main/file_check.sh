if cmp --silent -- "mr-wc-all" "mr-correct-wc.txt"; then
  echo "files contents are identical"
else
  echo "files differ"
fi