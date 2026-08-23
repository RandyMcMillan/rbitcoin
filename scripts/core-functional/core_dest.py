#!/usr/bin/env python3
"""Core DecodeDestination + bech32::LocateErrors dialect for harness validateaddress.

Ports enough of third_party/bitcoin/src/key_io.cpp and bech32.cpp for
rpc_invalid_address_message.py error strings and error_locations.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from test_framework.messages import hash256
from test_framework.script_util import (
    keyhash_to_p2pkh_script,
    scripthash_to_p2sh_script,
)
from test_framework.script import CScript, CScriptOp, OP_0, OP_1
from test_framework.segwit_addr import CHARSET, bech32_polymod, convertbits

BECH32_CHAR_LIMIT = 90
CHECKSUM_SIZE = 6
SEPARATOR = "1"
BECH32_WITNESS_PROG_MAX_LEN = 40
WITNESS_V1_TAPROOT_SIZE = 32
ANCHOR_BYTES = bytes((0x4E, 0x73))

# BIP173 charset reverse map for ASCII (both cases).
_CHARSET_REV = [-1] * 128
for _i, _c in enumerate(CHARSET):
    _CHARSET_REV[ord(_c)] = _i
    _CHARSET_REV[ord(_c.upper())] = _i

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_MAP_B58 = [-1] * 256
for _i, _c in enumerate(_B58):
    _MAP_B58[ord(_c)] = _i


def _gen_gf1024_tables() -> tuple[list[int], list[int]]:
    gf32_exp = [0] * 31
    gf32_log = [0] * 32
    fmod = 41
    gf32_exp[0] = 1
    gf32_log[0] = -1
    gf32_log[1] = 0
    v = 1
    for i in range(1, 31):
        v <<= 1
        if v & 32:
            v ^= fmod
        gf32_exp[i] = v
        gf32_log[v] = i

    gf1024_exp = [0] * 1023
    gf1024_log = [0] * 1024
    gf1024_exp[0] = 1
    gf1024_log[0] = -1
    gf1024_log[1] = 0
    v = 1
    for i in range(1, 1023):
        v0 = v & 31
        v1 = v >> 5
        v0n = gf32_exp[(gf32_log[v1] + gf32_log[23]) % 31] if v1 else 0
        v1n = (gf32_exp[(gf32_log[v1] + gf32_log[9]) % 31] if v1 else 0) ^ v0
        v = (v1n << 5) | v0n
        gf1024_exp[i] = v
        gf1024_log[v] = i
    return gf1024_exp, gf1024_log


GF1024_EXP, GF1024_LOG = _gen_gf1024_tables()


def _gen_syndrome_consts() -> list[int]:
    out = [0] * 25
    for k in range(1, 6):
        for shift in range(5):
            b = GF1024_LOG[1 << shift]
            c0 = GF1024_EXP[(997 * k + b) % 1023]
            c1 = GF1024_EXP[(998 * k + b) % 1023]
            c2 = GF1024_EXP[(999 * k + b) % 1023]
            out[5 * (k - 1) + shift] = (c2 << 20) | (c1 << 10) | c0
    return out


SYNDROME_CONSTS = _gen_syndrome_consts()


class _Enc(Enum):
    INVALID = 0
    BECH32 = 1
    BECH32M = 2


def _encoding_constant(enc: _Enc) -> int:
    return 1 if enc is _Enc.BECH32 else 0x2BC830A3


def _lower(c: str) -> str:
    o = ord(c)
    if 65 <= o <= 90:
        return chr(o - 65 + 97)
    return c


def _check_characters(s: str) -> list[int]:
    errors: list[int] = []
    lower = upper = False
    for i, ch in enumerate(s):
        c = ord(ch)
        if 97 <= c <= 122:
            if upper:
                errors.append(i)
            else:
                lower = True
        elif 65 <= c <= 90:
            if lower:
                errors.append(i)
            else:
                upper = True
        elif c < 33 or c > 126:
            errors.append(i)
    return errors


def _prepare_poly(hrp: str, values: list[int]) -> list[int]:
    ret = [ord(c) >> 5 for c in hrp]
    ret.append(0)
    ret.extend(ord(c) & 0x1F for c in hrp)
    ret.extend(values)
    return ret


def _verify_checksum(hrp: str, values: list[int]) -> _Enc:
    check = bech32_polymod(_prepare_poly(hrp, values))
    if check == _encoding_constant(_Enc.BECH32):
        return _Enc.BECH32
    if check == _encoding_constant(_Enc.BECH32M):
        return _Enc.BECH32M
    return _Enc.INVALID


def _bech32_decode(s: str, limit: int = BECH32_CHAR_LIMIT) -> tuple[_Enc, str, list[int]]:
    if _check_characters(s):
        return _Enc.INVALID, "", []
    if len(s) > limit:
        return _Enc.INVALID, "", []
    pos = s.rfind(SEPARATOR)
    if pos == -1 or pos == 0 or pos + CHECKSUM_SIZE >= len(s):
        return _Enc.INVALID, "", []
    values: list[int] = []
    for ch in s[pos + 1 :]:
        rev = _CHARSET_REV[ord(ch)] if ord(ch) < 128 else -1
        if rev == -1:
            return _Enc.INVALID, "", []
        values.append(rev)
    hrp = "".join(_lower(c) for c in s[:pos])
    enc = _verify_checksum(hrp, values)
    if enc is _Enc.INVALID:
        return _Enc.INVALID, "", []
    return enc, hrp, values[:-CHECKSUM_SIZE]


def _syndrome(residue: int) -> int:
    low = residue & 0x1F
    result = low ^ (low << 10) ^ (low << 20)
    for i in range(25):
        if (residue >> (5 + i)) & 1:
            result ^= SYNDROME_CONSTS[i]
    return result


def locate_errors(s: str, limit: int = BECH32_CHAR_LIMIT) -> tuple[str, list[int]]:
    if len(s) > limit:
        return "Bech32 string too long", list(range(limit, len(s)))

    char_errs = _check_characters(s)
    if char_errs:
        return "Invalid character or mixed case", char_errs

    pos = s.rfind(SEPARATOR)
    if pos == -1:
        return "Missing separator", []
    if pos == 0 or pos + CHECKSUM_SIZE >= len(s):
        return "Invalid separator position", [pos]

    hrp = "".join(_lower(c) for c in s[:pos])
    length = len(s) - 1 - pos
    values: list[int] = []
    for i in range(pos + 1, len(s)):
        c = ord(s[i])
        rev = _CHARSET_REV[c] if c < 128 else -1
        if rev == -1:
            return "Invalid Base 32 character", [i]
        values.append(rev)

    error_locations: list[int] = []
    error_encoding: _Enc | None = None
    for encoding in (_Enc.BECH32, _Enc.BECH32M):
        possible: list[int] = []
        enc = _prepare_poly(hrp, values)
        residue = bech32_polymod(enc) ^ _encoding_constant(encoding)
        if residue == 0:
            return "", []
        syn = _syndrome(residue)
        s0 = syn & 0x3FF
        s1 = (syn >> 10) & 0x3FF
        s2 = syn >> 20
        l_s0 = GF1024_LOG[s0]
        l_s1 = GF1024_LOG[s1]
        l_s2 = GF1024_LOG[s2]
        if (
            l_s0 != -1
            and l_s1 != -1
            and l_s2 != -1
            and (2 * l_s1 - l_s2 - l_s0 + 2046) % 1023 == 0
        ):
            p1 = (l_s1 - l_s0 + 1023) % 1023
            l_e1 = l_s0 + (1023 - 997) * p1
            if p1 < length and (l_e1 % 33) == 0:
                possible.append(len(s) - p1 - 1)
        else:
            for p1 in range(length):
                s2_s1p1 = s2 ^ (0 if s1 == 0 else GF1024_EXP[(l_s1 + p1) % 1023])
                if s2_s1p1 == 0:
                    continue
                l_s2_s1p1 = GF1024_LOG[s2_s1p1]
                s1_s0p1 = s1 ^ (0 if s0 == 0 else GF1024_EXP[(l_s0 + p1) % 1023])
                if s1_s0p1 == 0:
                    continue
                l_s1_s0p1 = GF1024_LOG[s1_s0p1]
                p2 = (l_s2_s1p1 - l_s1_s0p1 + 1023) % 1023
                if p2 >= length or p1 == p2:
                    continue
                s1_s0p2 = s1 ^ (0 if s0 == 0 else GF1024_EXP[(l_s0 + p2) % 1023])
                if s1_s0p2 == 0:
                    continue
                l_s1_s0p2 = GF1024_LOG[s1_s0p2]
                inv_p1_p2 = 1023 - GF1024_LOG[GF1024_EXP[p1] ^ GF1024_EXP[p2]]
                l_e2 = l_s1_s0p1 + inv_p1_p2 + (1023 - 997) * p2
                if l_e2 % 33:
                    continue
                l_e1 = l_s1_s0p2 + inv_p1_p2 + (1023 - 997) * p1
                if l_e1 % 33:
                    continue
                if p1 > p2:
                    possible.extend([len(s) - p1 - 1, len(s) - p2 - 1])
                else:
                    possible.extend([len(s) - p2 - 1, len(s) - p1 - 1])
                break

        if not error_locations or (possible and len(possible) < len(error_locations)):
            error_locations = possible
            if error_locations:
                error_encoding = encoding

    if error_encoding is _Enc.BECH32M:
        msg = "Invalid Bech32m checksum"
    elif error_encoding is _Enc.BECH32:
        msg = "Invalid Bech32 checksum"
    else:
        msg = "Invalid checksum"
    return msg, error_locations


def _decode_base58(s: str, max_ret: int) -> bytes | None:
    if "\0" in s:
        return None
    psz = s.lstrip()
    # trailing space check after decode loop
    zeroes = 0
    i = 0
    while i < len(psz) and psz[i] == "1":
        zeroes += 1
        if zeroes > max_ret:
            return None
        i += 1
    size = (len(psz) - i) * 733 // 1000 + 1
    b256 = bytearray(size)
    length = 0
    while i < len(psz) and not psz[i].isspace():
        carry = _MAP_B58[ord(psz[i])] if ord(psz[i]) < 256 else -1
        if carry == -1:
            return None
        j = 0
        for k in range(size - 1, -1, -1):
            if carry == 0 and j >= length:
                break
            carry += 58 * b256[k]
            b256[k] = carry % 256
            carry //= 256
            j += 1
        length = j
        if length + zeroes > max_ret:
            return None
        i += 1
    while i < len(psz) and psz[i].isspace():
        i += 1
    if i != len(psz):
        return None
    it = size - length
    return bytes(zeroes) + bytes(b256[it:])


def _decode_base58check(s: str, max_ret: int) -> bytes | None:
    if max_ret > (2**31 - 1) - 4:
        lim = 2**31 - 1
    else:
        lim = max_ret + 4
    data = _decode_base58(s, lim)
    if data is None or len(data) < 4:
        return None
    if hash256(data[:-4])[:4] != data[-4:]:
        return None
    return data[:-4]


def _witness_script(version: int, program: bytes) -> bytes:
    if version == 0:
        op = OP_0
    elif version == 1:
        op = OP_1
    else:
        op = CScriptOp.encode_op_n(version)
    return bytes(CScript([op, program]))


def _describe(
    *,
    isscript: bool | None,
    iswitness: bool,
    witness_version: int | None = None,
    witness_program: bytes | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isscript is not None:
        out["isscript"] = isscript
    out["iswitness"] = iswitness
    if witness_version is not None:
        out["witness_version"] = witness_version
    if witness_program is not None:
        out["witness_program"] = witness_program.hex()
    return out


def decode_destination(
    s: str,
    *,
    hrp: str = "bcrt",
    pubkey_prefix: bytes = b"\x6f",
    script_prefix: bytes = b"\xc4",
) -> tuple[dict[str, Any] | None, str, list[int]]:
    """Core DecodeDestination. Returns (detail, error_msg, error_locations).

    detail is None when invalid; otherwise Core-shaped valid fields sans isvalid.
    """
    is_bech32 = s[: len(hrp)].lower() == hrp

    if not is_bech32:
        data = _decode_base58check(s, 21)
        if data is not None:
            if len(data) == 20 + len(pubkey_prefix) and data.startswith(pubkey_prefix):
                payload = data[len(pubkey_prefix) :]
                spk = bytes(keyhash_to_p2pkh_script(payload))
                return (
                    {
                        "address": s,
                        "scriptPubKey": spk.hex(),
                        **_describe(isscript=False, iswitness=False),
                    },
                    "",
                    [],
                )
            if len(data) == 20 + len(script_prefix) and data.startswith(script_prefix):
                payload = data[len(script_prefix) :]
                spk = bytes(scripthash_to_p2sh_script(payload))
                return (
                    {
                        "address": s,
                        "scriptPubKey": spk.hex(),
                        **_describe(isscript=True, iswitness=False),
                    },
                    "",
                    [],
                )
            if (
                len(data) >= len(script_prefix) and data.startswith(script_prefix)
            ) or (len(data) >= len(pubkey_prefix) and data.startswith(pubkey_prefix)):
                return None, "Invalid length for Base58 address (P2PKH or P2SH)", []
            return None, "Invalid or unsupported Base58-encoded address.", []
        if _decode_base58(s, 100) is None:
            return None, "Invalid or unsupported Segwit (Bech32) or Base58 encoding.", []
        return None, "Invalid checksum or length of Base58 address (P2PKH or P2SH)", []

    enc, dec_hrp, dec_data = _bech32_decode(s)
    if enc in (_Enc.BECH32, _Enc.BECH32M):
        if not dec_data:
            return None, "Empty Bech32 data section", []
        if dec_hrp != hrp:
            return (
                None,
                f"Invalid or unsupported prefix for Segwit (Bech32) address (expected {hrp}, got {dec_hrp}).",
                [],
            )
        version = dec_data[0]
        if version == 0 and enc is not _Enc.BECH32:
            return None, "Version 0 witness address must use Bech32 checksum", []
        if version != 0 and enc is not _Enc.BECH32M:
            return None, "Version 1+ witness address must use Bech32m checksum", []
        prog = convertbits(dec_data[1:], 5, 8, False)
        if prog is not None:
            data = bytes(prog)
            byte_str = "byte" if len(data) == 1 else "bytes"
            if version == 0:
                if len(data) == 20:
                    spk = _witness_script(0, data)
                    return (
                        {
                            "address": s,
                            "scriptPubKey": spk.hex(),
                            **_describe(
                                isscript=False,
                                iswitness=True,
                                witness_version=0,
                                witness_program=data,
                            ),
                        },
                        "",
                        [],
                    )
                if len(data) == 32:
                    spk = _witness_script(0, data)
                    return (
                        {
                            "address": s,
                            "scriptPubKey": spk.hex(),
                            **_describe(
                                isscript=True,
                                iswitness=True,
                                witness_version=0,
                                witness_program=data,
                            ),
                        },
                        "",
                        [],
                    )
                return (
                    None,
                    f"Invalid Bech32 v0 address program size ({len(data)} {byte_str}), per BIP141",
                    [],
                )
            if version == 1 and len(data) == WITNESS_V1_TAPROOT_SIZE:
                spk = _witness_script(1, data)
                return (
                    {
                        "address": s,
                        "scriptPubKey": spk.hex(),
                        **_describe(
                            isscript=True,
                            iswitness=True,
                            witness_version=1,
                            witness_program=data,
                        ),
                    },
                    "",
                    [],
                )
            if version == 1 and data == ANCHOR_BYTES:
                spk = _witness_script(1, data)
                return (
                    {
                        "address": s,
                        "scriptPubKey": spk.hex(),
                        **_describe(isscript=True, iswitness=True),
                    },
                    "",
                    [],
                )
            if version > 16:
                return None, "Invalid Bech32 address witness version", []
            if len(data) < 2 or len(data) > BECH32_WITNESS_PROG_MAX_LEN:
                return (
                    None,
                    f"Invalid Bech32 address program size ({len(data)} {byte_str})",
                    [],
                )
            spk = _witness_script(version, data)
            return (
                {
                    "address": s,
                    "scriptPubKey": spk.hex(),
                    **_describe(
                        isscript=None,
                        iswitness=True,
                        witness_version=version,
                        witness_program=data,
                    ),
                },
                "",
                [],
            )
        return None, "Invalid padding in Bech32 data section", []

    err, locs = locate_errors(s)
    return None, err, locs
