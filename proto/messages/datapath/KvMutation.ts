// @ts-nocheck
import {
  Type as KvValue,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./KvValue.ts";
import {
  Type as KvMutationType,
  name2num,
  num2name,
} from "./KvMutationType.ts";
import {
  tsValueToJsonValueFns,
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
  tsValueToWireValueFns,
  wireValueToTsValueFns,
} from "../../runtime/wire/scalar.ts";
import {
  default as Long,
} from "../../runtime/Long.ts";
import {
  default as deserialize,
} from "../../runtime/wire/deserialize.ts";

export declare namespace $.datapath {
  export type KvMutation = {
    key: Uint8Array;
    value?: KvValue;
    mutationType: KvMutationType;
  }
}

export type Type = $.datapath.KvMutation;

export function getDefaultValue(): $.datapath.KvMutation {
  return {
    key: new Uint8Array(),
    value: undefined,
    mutationType: "M_UNSPECIFIED",
  };
}

export function createValue(partialValue: Partial<$.datapath.KvMutation>): $.datapath.KvMutation {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.datapath.KvMutation): unknown {
  const result: any = {};
  if (value.key !== undefined) result.key = tsValueToJsonValueFns.bytes(value.key);
  if (value.value !== undefined) result.value = encodeJson_1(value.value);
  if (value.mutationType !== undefined) result.mutationType = tsValueToJsonValueFns.enum(value.mutationType);
  return result;
}

export function decodeJson(value: any): $.datapath.KvMutation {
  const result = getDefaultValue();
  if (value.key !== undefined) result.key = jsonValueToTsValueFns.bytes(value.key);
  if (value.value !== undefined) result.value = decodeJson_1(value.value);
  if (value.mutationType !== undefined) result.mutationType = jsonValueToTsValueFns.enum(value.mutationType) as KvMutationType;
  return result;
}

export function encodeBinary(value: $.datapath.KvMutation): Uint8Array {
  const result: WireMessage = [];
  if (value.key !== undefined) {
    const tsValue = value.key;
    result.push(
      [1, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.value !== undefined) {
    const tsValue = value.value;
    result.push(
      [2, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  if (value.mutationType !== undefined) {
    const tsValue = value.mutationType;
    result.push(
      [3, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.datapath.KvMutation {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.key = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined;
    if (value === undefined) break field;
    result.value = value;
  }
  field: {
    const wireValue = wireFields.get(3);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.Varint ? num2name[wireValue.value[0] as keyof typeof num2name] : undefined;
    if (value === undefined) break field;
    result.mutationType = value;
  }
  return result;
}
