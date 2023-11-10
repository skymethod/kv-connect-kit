// @ts-nocheck
import {
  Type as ReadRange,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./ReadRange.ts";
import {
  jsonValueToTsValueFns,
} from "../../runtime/json/scalar.ts";
import {
  WireMessage,
  WireType,
} from "../../runtime/wire/index.ts";
import {
  default as serialize,
} from "../../runtime/wire/serialize.ts";
import {
  default as deserialize,
} from "../../runtime/wire/deserialize.ts";

export declare namespace $.datapath {
  export type SnapshotRead = {
    ranges: ReadRange[];
  }
}

export type Type = $.datapath.SnapshotRead;

export function getDefaultValue(): $.datapath.SnapshotRead {
  return {
    ranges: [],
  };
}

export function createValue(partialValue: Partial<$.datapath.SnapshotRead>): $.datapath.SnapshotRead {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.datapath.SnapshotRead): unknown {
  const result: any = {};
  result.ranges = value.ranges.map(value => encodeJson_1(value));
  return result;
}

export function decodeJson(value: any): $.datapath.SnapshotRead {
  const result = getDefaultValue();
  result.ranges = value.ranges?.map((value: any) => decodeJson_1(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.datapath.SnapshotRead): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.ranges) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.datapath.SnapshotRead {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.ranges = value as any;
  }
  return result;
}
