// @ts-nocheck
export declare namespace $.datapath {
  export type AtomicWriteStatus =
    | "AW_UNSPECIFIED"
    | "AW_SUCCESS"
    | "AW_CHECK_FAILURE"
    | "AW_UNSUPPORTED_WRITE"
    | "AW_USAGE_LIMIT_EXCEEDED"
    | "AW_WRITE_DISABLED"
    | "AW_QUEUE_BACKLOG_LIMIT_EXCEEDED";
}

export type Type = $.datapath.AtomicWriteStatus;

export const num2name = {
  0: "AW_UNSPECIFIED",
  1: "AW_SUCCESS",
  2: "AW_CHECK_FAILURE",
  3: "AW_UNSUPPORTED_WRITE",
  4: "AW_USAGE_LIMIT_EXCEEDED",
  5: "AW_WRITE_DISABLED",
  6: "AW_QUEUE_BACKLOG_LIMIT_EXCEEDED",
} as const;

export const name2num = {
  AW_UNSPECIFIED: 0,
  AW_SUCCESS: 1,
  AW_CHECK_FAILURE: 2,
  AW_UNSUPPORTED_WRITE: 3,
  AW_USAGE_LIMIT_EXCEEDED: 4,
  AW_WRITE_DISABLED: 5,
  AW_QUEUE_BACKLOG_LIMIT_EXCEEDED: 6,
} as const;
