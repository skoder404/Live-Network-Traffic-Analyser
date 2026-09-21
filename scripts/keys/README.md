# scripts/keys/

Place the public SSH keys (`id_ed25519.pub` or `id_rsa.pub`) of each team member here:
- `naveena.pub`
- `rithika.pub`
- `sushil.pub`
- `priyan.pub`
- `yashwant.pub`

When `sudo bash scripts/setup_remote_access.sh` is executed on Laptop A, these keys will automatically be installed into each user's `~/.ssh/authorized_keys` file with secure permissions.

**Security Rule:** Never commit private keys (`id_ed25519` / `id_rsa`) to this or any repository.
