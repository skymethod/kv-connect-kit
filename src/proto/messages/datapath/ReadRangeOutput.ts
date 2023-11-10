// @ts-nocheck
import {
  Type as KvEntry,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./KvEntry.ts";
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
  export type ReadRangeOutput = {
    values: KvEntry[];
  }
}

export type Type = $.datapath.ReadRangeOutput;

export function getDefaultValue(): $.datapath.ReadRangeOutput {
  return {
    values: [],
  };
}

export function createValue(partialValue: Partial<$.datapath.ReadRangeOutput>): $.datapath.ReadRangeOutput {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.datapath.ReadRangeOutput): unknown {
  const result: any = {};
  result.values = value.values.map(value => encodeJson_1(value));
  return result;
}

export function decodeJson(value: any): $.datapath.ReadRangeOutput {
  const result = getDefaultValue();
  result.values = value.values?.map((value: any) => decodeJson_1(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.datapath.ReadRangeOutput): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.values) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.datapath.ReadRangeOutput {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.values = value as any;
  }
  return result;
}
