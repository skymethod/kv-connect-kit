// @ts-nocheck
import {
  Type as KvCheck,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./KvCheck.ts";
import {
  Type as KvMutation,
  encodeJson as encodeJson_2,
  decodeJson as decodeJson_2,
  encodeBinary as encodeBinary_2,
  decodeBinary as decodeBinary_2,
} from "./KvMutation.ts";
import {
  Type as Enqueue,
  encodeJson as encodeJson_3,
  decodeJson as decodeJson_3,
  encodeBinary as encodeBinary_3,
  decodeBinary as decodeBinary_3,
} from "./Enqueue.ts";
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
  export type AtomicWrite = {
    kvChecks: KvCheck[];
    kvMutations: KvMutation[];
    enqueues: Enqueue[];
  }
}

export type Type = $.datapath.AtomicWrite;

export function getDefaultValue(): $.datapath.AtomicWrite {
  return {
    kvChecks: [],
    kvMutations: [],
    enqueues: [],
  };
}

export function createValue(partialValue: Partial<$.datapath.AtomicWrite>): $.datapath.AtomicWrite {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.datapath.AtomicWrite): unknown {
  const result: any = {};
  result.kvChecks = value.kvChecks.map(value => encodeJson_1(value));
  result.kvMutations = value.kvMutations.map(value => encodeJson_2(value));
  result.enqueues = value.enqueues.map(value => encodeJson_3(value));
  return result;
}

export function decodeJson(value: any): $.datapath.AtomicWrite {
  const result = getDefaultValue();
  result.kvChecks = value.kvChecks?.map((value: any) => decodeJson_1(value)) ?? [];
  result.kvMutations = value.kvMutations?.map((value: any) => decodeJson_2(value)) ?? [];
  result.enqueues = value.enqueues?.map((value: any) => decodeJson_3(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.datapath.AtomicWrite): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.kvChecks) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  for (const tsValue of value.kvMutations) {
    result.push(
      [2, { type: WireType.LengthDelimited as const, value: encodeBinary_2(tsValue) }],
    );
  }
  for (const tsValue of value.enqueues) {
    result.push(
      [3, { type: WireType.LengthDelimited as const, value: encodeBinary_3(tsValue) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.datapath.AtomicWrite {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.kvChecks = value as any;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 2).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_2(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.kvMutations = value as any;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 3).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_3(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.enqueues = value as any;
  }
  return result;
}
