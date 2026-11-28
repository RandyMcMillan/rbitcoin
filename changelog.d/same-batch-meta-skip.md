Changed

- Structural spentness does not read a spend slot whose parent this
  confirm batch appended. That slot is still empty. A parent from an
  earlier batch, including one this batch is connecting again, is still
  read.
