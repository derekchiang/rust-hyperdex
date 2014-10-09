/* automatically generated by rust-bindgen */

pub type int8_t = ::libc::c_char;
pub type int16_t = ::libc::c_short;
pub type int32_t = ::libc::c_int;
pub type int64_t = ::libc::c_long;
pub type uint8_t = ::libc::c_uchar;
pub type uint16_t = ::libc::c_ushort;
pub type uint32_t = ::libc::c_uint;
pub type uint64_t = ::libc::c_ulong;
pub type int_least8_t = ::libc::c_char;
pub type int_least16_t = ::libc::c_short;
pub type int_least32_t = ::libc::c_int;
pub type int_least64_t = ::libc::c_long;
pub type uint_least8_t = ::libc::c_uchar;
pub type uint_least16_t = ::libc::c_ushort;
pub type uint_least32_t = ::libc::c_uint;
pub type uint_least64_t = ::libc::c_ulong;
pub type int_fast8_t = ::libc::c_char;
pub type int_fast16_t = ::libc::c_long;
pub type int_fast32_t = ::libc::c_long;
pub type int_fast64_t = ::libc::c_long;
pub type uint_fast8_t = ::libc::c_uchar;
pub type uint_fast16_t = ::libc::c_ulong;
pub type uint_fast32_t = ::libc::c_ulong;
pub type uint_fast64_t = ::libc::c_ulong;
pub type intptr_t = ::libc::c_long;
pub type uintptr_t = ::libc::c_ulong;
pub type intmax_t = ::libc::c_long;
pub type uintmax_t = ::libc::c_ulong;
pub type Enum_hyperdatatype = ::libc::c_uint;
pub static HYPERDATATYPE_GENERIC: ::libc::c_uint = 9216;
pub static HYPERDATATYPE_STRING: ::libc::c_uint = 9217;
pub static HYPERDATATYPE_INT64: ::libc::c_uint = 9218;
pub static HYPERDATATYPE_FLOAT: ::libc::c_uint = 9219;
pub static HYPERDATATYPE_DOCUMENT: ::libc::c_uint = 9223;
pub static HYPERDATATYPE_LIST_GENERIC: ::libc::c_uint = 9280;
pub static HYPERDATATYPE_LIST_STRING: ::libc::c_uint = 9281;
pub static HYPERDATATYPE_LIST_INT64: ::libc::c_uint = 9282;
pub static HYPERDATATYPE_LIST_FLOAT: ::libc::c_uint = 9283;
pub static HYPERDATATYPE_SET_GENERIC: ::libc::c_uint = 9344;
pub static HYPERDATATYPE_SET_STRING: ::libc::c_uint = 9345;
pub static HYPERDATATYPE_SET_INT64: ::libc::c_uint = 9346;
pub static HYPERDATATYPE_SET_FLOAT: ::libc::c_uint = 9347;
pub static HYPERDATATYPE_MAP_GENERIC: ::libc::c_uint = 9408;
pub static HYPERDATATYPE_MAP_STRING_KEYONLY: ::libc::c_uint = 9416;
pub static HYPERDATATYPE_MAP_STRING_STRING: ::libc::c_uint = 9417;
pub static HYPERDATATYPE_MAP_STRING_INT64: ::libc::c_uint = 9418;
pub static HYPERDATATYPE_MAP_STRING_FLOAT: ::libc::c_uint = 9419;
pub static HYPERDATATYPE_MAP_INT64_KEYONLY: ::libc::c_uint = 9424;
pub static HYPERDATATYPE_MAP_INT64_STRING: ::libc::c_uint = 9425;
pub static HYPERDATATYPE_MAP_INT64_INT64: ::libc::c_uint = 9426;
pub static HYPERDATATYPE_MAP_INT64_FLOAT: ::libc::c_uint = 9427;
pub static HYPERDATATYPE_MAP_FLOAT_KEYONLY: ::libc::c_uint = 9432;
pub static HYPERDATATYPE_MAP_FLOAT_STRING: ::libc::c_uint = 9433;
pub static HYPERDATATYPE_MAP_FLOAT_INT64: ::libc::c_uint = 9434;
pub static HYPERDATATYPE_MAP_FLOAT_FLOAT: ::libc::c_uint = 9435;
pub static HYPERDATATYPE_GARBAGE: ::libc::c_uint = 9727;
pub type Enum_hyperpredicate = ::libc::c_uint;
pub static HYPERPREDICATE_FAIL: ::libc::c_uint = 9728;
pub static HYPERPREDICATE_EQUALS: ::libc::c_uint = 9729;
pub static HYPERPREDICATE_LESS_THAN: ::libc::c_uint = 9738;
pub static HYPERPREDICATE_LESS_EQUAL: ::libc::c_uint = 9730;
pub static HYPERPREDICATE_GREATER_EQUAL: ::libc::c_uint = 9731;
pub static HYPERPREDICATE_GREATER_THAN: ::libc::c_uint = 9739;
pub static HYPERPREDICATE_CONTAINS_LESS_THAN: ::libc::c_uint = 9732;
pub static HYPERPREDICATE_REGEX: ::libc::c_uint = 9733;
pub static HYPERPREDICATE_LENGTH_EQUALS: ::libc::c_uint = 9734;
pub static HYPERPREDICATE_LENGTH_LESS_EQUAL: ::libc::c_uint = 9735;
pub static HYPERPREDICATE_LENGTH_GREATER_EQUAL: ::libc::c_uint = 9736;
pub static HYPERPREDICATE_CONTAINS: ::libc::c_uint = 9737;
pub enum Struct_hyperspace { }
pub type Enum_hyperspace_returncode = ::libc::c_uint;
pub static HYPERSPACE_SUCCESS: ::libc::c_uint = 8576;
pub static HYPERSPACE_INVALID_NAME: ::libc::c_uint = 8577;
pub static HYPERSPACE_INVALID_TYPE: ::libc::c_uint = 8578;
pub static HYPERSPACE_DUPLICATE: ::libc::c_uint = 8579;
pub static HYPERSPACE_IS_KEY: ::libc::c_uint = 8580;
pub static HYPERSPACE_UNKNOWN_ATTR: ::libc::c_uint = 8581;
pub static HYPERSPACE_NO_SUBSPACE: ::libc::c_uint = 8582;
pub static HYPERSPACE_OUT_OF_BOUNDS: ::libc::c_uint = 8583;
pub static HYPERSPACE_UNINDEXABLE: ::libc::c_uint = 8584;
pub static HYPERSPACE_GARBAGE: ::libc::c_uint = 8703;
extern "C" {
    pub fn hyperspace_create() -> *mut Struct_hyperspace;
    pub fn hyperspace_parse(desc: *const ::libc::c_char) ->
     *mut Struct_hyperspace;
    pub fn hyperspace_destroy(space: *mut Struct_hyperspace);
    pub fn hyperspace_error(space: *mut Struct_hyperspace) ->
     *const ::libc::c_char;
    pub fn hyperspace_set_name(space: *mut Struct_hyperspace,
                               name: *const ::libc::c_char) ->
     Enum_hyperspace_returncode;
    pub fn hyperspace_set_key(space: *mut Struct_hyperspace,
                              attr: *const ::libc::c_char,
                              datatype: Enum_hyperdatatype) ->
     Enum_hyperspace_returncode;
    pub fn hyperspace_add_attribute(space: *mut Struct_hyperspace,
                                    attr: *const ::libc::c_char,
                                    datatype: Enum_hyperdatatype) ->
     Enum_hyperspace_returncode;
    pub fn hyperspace_add_subspace(space: *mut Struct_hyperspace) ->
     Enum_hyperspace_returncode;
    pub fn hyperspace_add_subspace_attribute(space: *mut Struct_hyperspace,
                                             attr: *const ::libc::c_char) ->
     Enum_hyperspace_returncode;
    pub fn hyperspace_add_index(space: *mut Struct_hyperspace,
                                attr: *const ::libc::c_char) ->
     Enum_hyperspace_returncode;
    pub fn hyperspace_set_fault_tolerance(space: *mut Struct_hyperspace,
                                          num: uint64_t) ->
     Enum_hyperspace_returncode;
    pub fn hyperspace_set_number_of_partitions(space: *mut Struct_hyperspace,
                                               num: uint64_t) ->
     Enum_hyperspace_returncode;
}
