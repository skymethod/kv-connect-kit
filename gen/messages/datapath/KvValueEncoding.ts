// @ts-nocheck
export declare namespace $.datapath {
  export type KvValueEncoding =
    | "VE_UNSPECIFIED"
    | "VE_V8"
    | "VE_LE64"
    | "VE_BYTES";
}

export type Type = $.datapath.KvValueEncoding;

export const num2name = {
  0: "VE_UNSPECIFIED",
  1: "VE_V8",
  2: "VE_LE64",
  3: "VE_BYTES",
} as const;

export const name2num = {
  VE_UNSPECIFIED: 0,
  VE_V8: 1,
  VE_LE64: 2,
  VE_BYTES: 3,
} as const;
