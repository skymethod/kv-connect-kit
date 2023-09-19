// @ts-nocheck
export declare namespace $.datapath {
  export type KvMutationType =
    | "M_UNSPECIFIED"
    | "M_SET"
    | "M_CLEAR"
    | "M_SUM"
    | "M_MAX"
    | "M_MIN";
}

export type Type = $.datapath.KvMutationType;

export const num2name = {
  0: "M_UNSPECIFIED",
  1: "M_SET",
  2: "M_CLEAR",
  3: "M_SUM",
  4: "M_MAX",
  5: "M_MIN",
} as const;

export const name2num = {
  M_UNSPECIFIED: 0,
  M_SET: 1,
  M_CLEAR: 2,
  M_SUM: 3,
  M_MAX: 4,
  M_MIN: 5,
} as const;
