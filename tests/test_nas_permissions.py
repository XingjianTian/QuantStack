import re
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts/nas/quantstack-permissions"


def test_nas_permission_helper_is_fixed_scope_and_least_privilege() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert 'config_path="/etc/quantstack-permissions.conf"' in source
    assert 'shadow_root="$data_root/shadow-runs"' in source
    assert "t[0-9][0-9][0-9])" in source
    assert "NOPASSWD: ALL" not in source
    assert "chmod 777" not in source
    assert "chmod 0777" not in source
    assert 'id "$operator_user" >/dev/null 2>&1' in source
    assert 'getent passwd "$operator_user"' not in source
    assert "install -o root -g root -m 0644 \"$temporary_config\" \"$config_path\"" in source
    assert 'echo "$operator_user ALL=(root) NOPASSWD: $installed_path repair-read"' in source
    assert 'echo "$operator_user ALL=(root) NOPASSWD: $installed_path provision-shadow *"' in source


def test_nas_permission_helper_preserves_owners_on_authoritative_data() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    repair = source.split("repair_read_tree() {", 1)[1].split("validate_task_id() {", 1)[0]
    migration = source.split("migrate_read_tree_groups() {", 1)[1].split(
        "repair_read_tree() {", 1
    )[0]

    assert '"$data_root/canonical"' in repair
    assert '"$data_root/catalog"' in repair
    assert '"$data_root/staging/proposed_manifests"' in repair
    assert "chown" not in repair
    assert "chgrp" not in repair
    assert "chgrp -R" in migration
    assert 'find "$tree" ! -gid "$expected_gid" -print -quit' in repair
    assert "chmod 0640" not in repair
    assert "chmod 2750" not in repair
    install_model = source.split("install_model() {", 1)[1].split("status() {", 1)[0]
    assert "migrate_read_tree_groups" not in install_model
    assert "repair_read_tree" not in install_model


def _acl_spec(source: str, *, directory: bool, default: bool = False) -> str:
    kind = "d" if directory else "f"
    default_flag = r" -d" if default else ""
    match = re.search(
        rf"find \"\$tree\" -type {kind} -exec setfacl{default_flag} -m '([^']+)'",
        source,
    )
    assert match is not None
    return match.group(1)


def _apply_acl_spec(acl: dict[str, str], spec: str) -> dict[str, str]:
    result = dict(acl)
    key_map = {"g::": "group", "m::": "mask", "o::": "other"}
    for entry in spec.split(","):
        prefix = entry[:3]
        assert prefix in key_map
        result[key_map[prefix]] = entry[3:]
    return result


def _effective(perms: str, mask: str) -> str:
    return "".join(char if char in perms and char in mask else "-" for char in "rwx")


def test_repair_read_tree_normalizes_contradictory_extended_file_acl() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    before = {
        "owner": "rw-",
        "named_user_1004": "r-x",
        "group": "-w-",
        "mask": "r--",
        "other": "---",
    }

    after = _apply_acl_spec(before, _acl_spec(source, directory=False))

    assert after["group"] == "r--"
    assert after["mask"] == "r--"
    assert _effective(after["group"], after["mask"]) == "r--"
    assert after["named_user_1004"] == before["named_user_1004"]
    assert after["owner"] == before["owner"]
    assert after["other"] == "---"


def test_repair_read_tree_sets_directory_access_and_default_acl_contract() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert _acl_spec(source, directory=True) == "g::r-x,m::r-x,o::---"
    assert _acl_spec(source, directory=True, default=True) == "g::r-x,m::r-x,o::---"
    assert "chmod g+s" in source


def test_repair_read_tree_requires_acl_tools_and_avoids_uid_exceptions() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    repair = source.split("repair_read_tree() {", 1)[1].split("validate_task_id() {", 1)[0]

    assert 'command -v setfacl >/dev/null 2>&1 || fail "setfacl is required"' in source
    assert 'command -v getfacl >/dev/null 2>&1 || fail "getfacl is required"' in source
    assert "u:1000" not in repair
    assert "-b" not in repair
    assert "--remove-all" not in repair


def test_scoped_cn_equity_repair_is_snapshot_canary_then_full_repair() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    repair = source.split("repair_cn_equity_version() {", 1)[1].split(
        "validate_task_id() {", 1
    )[0]

    assert 'tree="$data_root/canonical/cn_equity_daily/version=$version"' in repair
    assert "getfacl -R -p" in repair
    assert 'setfacl --restore="$snapshot"' in repair
    assert repair.index("getfacl -R -p") < repair.index("# One-file canary first")
    assert repair.index('setfacl -m \'g::r--,m::r--,o::---\' -- "$bars"') < repair.index(
        'find "$tree" -xdev -type f -exec setfacl'
    )
    assert "setpriv --reuid 1000 --regid 1000" in repair
    assert "restore_needed=0" in repair
    assert 'find "$tree" -xdev ! -gid "$expected_gid"' in repair
    assert "*[!A-Za-z0-9._-]*" in source


def test_install_authorizes_only_the_scoped_version_repair_argument() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert "repair-cn-equity-version *" in source
    assert '[ "$#" -eq 2 ] || fail "usage: $0 repair-cn-equity-version VERSION"' in source


def _projection_helper(source: str) -> str:
    return source.split("prepare_shadow_projection() {", 1)[1].split(
        "install_model() {", 1
    )[0]


def test_projection_preparation_is_fixed_to_two_validated_shadow_tasks() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    helper = _projection_helper(source)

    assert (
        '[ "$#" -eq 2 ] || fail '
        '"prepare-shadow-projection requires source and target task ids"'
    ) in helper
    assert 'validate_task_id "$source_task"' in helper
    assert 'validate_task_id "$target_task"' in helper
    assert '[ "$source_task" != "$target_task" ]' in helper
    assert 'source_task_root="$shadow_root/$source_task"' in helper
    assert 'target_task_root="$shadow_root/$target_task"' in helper
    assert 'source_quant_data="$source_task_root/quant-data"' in helper
    assert 'target_quant_data="$target_task_root/quant-data"' in helper
    assert (
        '[ "$#" -eq 3 ] || fail '
        '"usage: $0 prepare-shadow-projection SOURCE_TNNN TARGET_TNNN"'
    ) in source
    assert "prepare_shadow_projection \"$2\" \"$3\"" in source


def test_projection_preparation_preserves_protected_parent_contract() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    helper = _projection_helper(source)
    parent_check = source.split("require_shadow_task_parent() {", 1)[1].split(
        "prepare_shadow_projection() {", 1
    )[0]

    assert '[ "$(stat -c %u "$task_root")" -eq 0 ]' in parent_check
    assert '[ "$(stat -c %g "$task_root")" -eq "$expected_shadow_gid" ]' in parent_check
    assert '[ "$(stat -c %a "$task_root")" = 2750 ]' in parent_check
    assert "chmod" not in helper
    assert "chown" not in helper
    assert "chgrp" not in helper
    assert "setfacl" not in helper


def test_projection_preparation_checks_empty_leaf_before_staged_replace() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    helper = _projection_helper(source)

    uid_check = '[ "$(stat -c %u "$target_quant_data")" -eq "$expected_operator_uid" ]'
    gid_check = '[ "$(stat -c %g "$target_quant_data")" -eq "$expected_shadow_gid" ]'
    mode_check = '[ "$(stat -c %a "$target_quant_data")" = 2770 ]'
    empty_check = 'find "$target_quant_data" -mindepth 1 -maxdepth 1 -print -quit'
    copy = 'cp -a --reflink=always -- "$source_quant_data" "$staged_quant_data"'
    remove = 'rmdir "$target_quant_data"'
    replace = 'mv -- "$staged_quant_data" "$target_quant_data"'

    for check in (uid_check, gid_check, mode_check, empty_check):
        assert check in helper
        assert helper.index(check) < helper.index(copy)
    assert helper.index(copy) < helper.index(remove) < helper.index(replace)
    assert 'staged_quant_data="$target_task_root/.quant-data.prepare"' in helper


def test_projection_preparation_rolls_back_only_the_empty_target_leaf() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    helper = _projection_helper(source)

    assert 'if [ "$target_removed" -eq 1 ] && [ ! -e "$target_quant_data" ]; then' in helper
    assert (
        'install -d -o "$operator_user" -g "$shadow_group" '
        '-m 2770 "$target_quant_data"'
    ) in helper
    assert 'if [ "$stage_started" -eq 1 ] && [ -e "$staged_quant_data" ]; then' in helper
    assert 'rm -rf -- "$staged_quant_data"' in helper
    assert 'rm -rf -- "$target_quant_data"' not in helper
    assert "trap rollback_projection EXIT HUP INT TERM" in helper


def test_install_authorizes_only_named_projection_preparation_action() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert "NOPASSWD: ALL" not in source
    assert (
        'echo "$operator_user ALL=(root) NOPASSWD: $installed_path '
        'prepare-shadow-projection *"'
    ) in source


def test_projection_preparation_binds_exact_shell_operand_forms() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    helper = _projection_helper(source)
    parent_check = source.split("require_shadow_task_parent() {", 1)[1].split(
        "prepare_shadow_projection() {", 1
    )[0]

    parent_lines = {line.strip() for line in parent_check.splitlines()}
    assert (
        '[ "$(stat -c %g "$task_root")" -eq "$expected_shadow_gid" ] '
        '|| fail "$description has an unexpected gid"'
    ) in parent_lines
    expected_helper_lines = {
        '[ "$(stat -c %u "$target_quant_data")" -eq "$expected_operator_uid" ] '
        '|| fail "target quant-data has an unexpected uid"',
        '[ "$(stat -c %g "$target_quant_data")" -eq "$expected_shadow_gid" ] '
        '|| fail "target quant-data has an unexpected gid"',
        '[ "$(stat -c %a "$target_quant_data")" = 2770 ] '
        '|| fail "target quant-data mode must be 2770"',
        '[ ! -e "$staged_quant_data" ] && [ ! -L "$staged_quant_data" ] '
        '|| fail "projection staging path already exists"',
        'install -d -o "$operator_user" -g "$shadow_group" -m 2770 '
        '"$target_quant_data" || status=1',
    }
    helper_lines = {line.strip() for line in helper.splitlines()}
    assert expected_helper_lines <= helper_lines
    assert "] + ||" not in helper
    assert '"$target_quant_data" +' not in helper
