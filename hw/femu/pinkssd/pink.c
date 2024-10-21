#include "hw/femu/nvme.h"
#include "hw/femu/kvssd/pink/pink_ftl.h"

static void pink_init_ctrl_str(FemuCtrl *n)
{
    static int fsid_vpink = 0;
    const char *vpinkssd_mn = "FEMU KeyValue-SSD Controller";
    const char *vpinkssd_sn = "vPINKSSD";

    nvme_set_ctrl_name(n, vpinkssd_mn, vpinkssd_sn, &fsid_vpink);
}

/* kv <=> key-value */
static void pink_init(FemuCtrl *n, Error **errp)
{
    struct ssd *ssd = n->ssd = g_malloc0(sizeof(struct ssd));

    pink_init_ctrl_str(n);
    kvssd_init_latency(&ssd->lat, TLC);

    ssd->dataplane_started_ptr = &n->dataplane_started;
    ssd->ssdname = (char *)n->devname;
    kv_debug("Starting FEMU in PinK-SSD mode ...\n");
    pinkssd_init(n);

    n->current_handle = 0;
    n->iterate_idx = 0;
    ssd->n = n;
}

static void pink_flip(FemuCtrl *n, NvmeCmd *cmd)
{
    struct ssd *ssd = n->ssd;
    int64_t cdw10 = le64_to_cpu(cmd->cdw10);

    switch (cdw10) {
    case FEMU_ENABLE_GC_DELAY:
        ssd->sp.enable_gc_delay = true;
        kv_log("%s,FEMU GC Delay Emulation [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_GC_DELAY:
        ssd->sp.enable_gc_delay = false;
        kv_log("%s,FEMU GC Delay Emulation [Disabled]!\n", n->devname);
        break;
    case FEMU_ENABLE_DELAY_EMU:
        kv_log("%s,FEMU Delay Emulation [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_DELAY_EMU:
        kv_log("%s,FEMU Delay Emulation [Disabled]!\n", n->devname);
        break;
    case FEMU_RESET_ACCT:
        n->nr_tt_ios = 0;
        n->nr_tt_late_ios = 0;
        kv_log("%s,Reset tt_late_ios/tt_ios,%lu/%lu\n", n->devname,
                n->nr_tt_late_ios, n->nr_tt_ios);
        break;
    case FEMU_ENABLE_LOG:
        n->print_log = true;
        kv_log("%s,Log print [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_LOG:
        n->print_log = false;
        kv_log("%s,Log print [Disabled]!\n", n->devname);
        break;
    default:
        printf("FEMU:%s,Not implemented flip cmd (%lu)\n", n->devname, cdw10);
    }
}

static uint16_t pink_nvme_rw(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                           NvmeRequest *req)
{
    return nvme_rw(n, ns, cmd, req);
}

static uint16_t pink_io_cmd(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                          NvmeRequest *req)
{
    /*kv_debug("cmd: 0x%x, mptr: 0x%lx, prp1: 0x%lx, prp2: 0x%lx\n", cmd->opcode,
      le64_to_cpu(cmd->mptr), le64_to_cpu(cmd->dptr.prp1),
      le64_to_cpu(cmd->dptr.prp2));
      kv_debug(
      "cdw10: 0x%x, cdw11: 0x%x, cdw12: 0x%x, cdw13: 0x%x, cdw14: 0x%x, cdw15: "
      "0x%x\n",
      le32_to_cpu(cmd->cdw10), le32_to_cpu(cmd->cdw11), le32_to_cpu(cmd->cdw12),
      le32_to_cpu(cmd->cdw13), le32_to_cpu(cmd->cdw14),
      le32_to_cpu(cmd->cdw15));*/
    uint32_t value_length;
    uint32_t key_length;
    uint32_t allocated_key_length;
    uint8_t *key;
    uint8_t *value;
    uint64_t prp1, prp2;
    uint64_t key_prp1, key_prp2;
    uint16_t status;
    switch (cmd->opcode) {
        case NVME_CMD_READ:
        case NVME_CMD_WRITE:
            return pink_nvme_rw(n, ns, cmd, req);
        case NVME_CMD_KV_STORE:
            value_length = le32_to_cpu(cmd->cdw10) * 4;
            key_length = (le32_to_cpu(cmd->cdw11) & 0xFF) + 1;
            allocated_key_length = 16;
            if (allocated_key_length < key_length) allocated_key_length = key_length;
            key = g_malloc0(allocated_key_length + 1);
            value = g_malloc0(value_length);
            prp1 = le64_to_cpu(cmd->dptr.prp1);
            prp2 = le64_to_cpu(cmd->dptr.prp2);
            key_prp1 =
                le32_to_cpu(cmd->cdw12) + ((uint64_t)le32_to_cpu(cmd->cdw13) << 32);
            key_prp2 =
                le32_to_cpu(cmd->cdw14) + ((uint64_t)le32_to_cpu(cmd->cdw15) << 32);
            status = NVME_SUCCESS;

            if (key_length <= 16) {
                *((uint32_t *)key) = le32_to_cpu(cmd->cdw12);
                *((uint32_t *)(key + 4)) = le32_to_cpu(cmd->cdw13);
                *((uint32_t *)(key + 8)) = le32_to_cpu(cmd->cdw14);
                *((uint32_t *)(key + 12)) = le32_to_cpu(cmd->cdw15);
            } else {
                /*
                kv_debug("key_length: 0x%x, key_prp1: 0x%lx, key_prp2: 0x%lx\n",
                        key_length, key_prp1, key_prp2);
                        */
                status = dma_write_prp(n, key, key_length, key_prp1, key_prp2);
                if (status != NVME_SUCCESS) {
                    return status;  // XXX free the resources
                }
            }
#ifdef HASH_COLLISION_MODELING
            memcpy(key+4, key, 4);
            memcpy(key+8, key, 4);
            memcpy(key+2, key+7, 4);
#endif
            /*
            kv_debug("keylen: %d: ", key_length);
            for (uint32_t i = 0; i < key_length; ++i) {
                kv_debug("%x", key[i]);
            }
            kv_debug("\n");
            */

            status = dma_write_prp(n, value, value_length, prp1, prp2);
            if (status != NVME_SUCCESS) {
                return status;  // XXX free the resources
            }
            /*for (uint32_t i = 0; i < key_length; ++i) {
              kv_debug("%x", key[i]);
              }
              kv_debug("\n");*/
            req->key_length = key_length;
            memset(req->key_buf, 0, key_length + 1);
            memcpy(req->key_buf, key, key_length);
            req->value_length = value_length;
            g_free(value);
            g_free(key);
            return NVME_SUCCESS;
        case NVME_CMD_KV_RETRIEVE:
            qatomic_inc(&n->pending_reads);
            value_length = le32_to_cpu(cmd->cdw10) * 4;
            key_length = (le32_to_cpu(cmd->cdw11) & 0xFF) + 1;
            allocated_key_length = 16;
            if (allocated_key_length < key_length) allocated_key_length = key_length;
            key = g_malloc0(allocated_key_length + 1);
            value = g_malloc0(value_length);
            prp1 = le64_to_cpu(cmd->dptr.prp1);
            prp2 = le64_to_cpu(cmd->dptr.prp2);
            key_prp1 =
                le32_to_cpu(cmd->cdw12) + ((uint64_t)le32_to_cpu(cmd->cdw13) << 32);
            key_prp2 =
                le32_to_cpu(cmd->cdw14) + ((uint64_t)le32_to_cpu(cmd->cdw15) << 32);
            status = NVME_SUCCESS;

            if (key_length <= 16) {
                *((uint32_t *)key) = le32_to_cpu(cmd->cdw12);
                *((uint32_t *)(key + 4)) = le32_to_cpu(cmd->cdw13);
                *((uint32_t *)(key + 8)) = le32_to_cpu(cmd->cdw14);
                *((uint32_t *)(key + 12)) = le32_to_cpu(cmd->cdw15);
            } else {
                /*
                kv_debug("key_length: 0x%x, key_prp1: 0x%lx, key_prp2: 0x%lx\n",
                        key_length, key_prp1, key_prp2);
                        */
                status = dma_write_prp(n, key, key_length, key_prp1, key_prp2);
                if (status != NVME_SUCCESS) {
                    return status;  // XXX free the resources
                }
            }
#ifdef HASH_COLLISION_MODELING
            memcpy(key+4, key, 4);
            memcpy(key+8, key, 4);
            memcpy(key+2, key+7, 4);
#endif
            /*
            kv_debug("keylen: %d: ", key_length);
            for (uint32_t i = 0; i < key_length; ++i) {
                kv_debug("%x", key[i]);
            }
            kv_debug("\n");
            */
            req->key_length = key_length;
            memset(req->key_buf, 0, key_length + 1);
            memcpy(req->key_buf, key, key_length);
            g_free(value);
            g_free(key);
            return NVME_SUCCESS;
        case NVME_CMD_KV_DELETE:
            return NVME_INVALID_OPCODE | NVME_DNR;
        case NVME_CMD_KV_ITERATE_REQUEST:
            return NVME_INVALID_OPCODE | NVME_DNR;
        case NVME_CMD_KV_ITERATE_READ:
            return NVME_INVALID_OPCODE | NVME_DNR;
        case NVME_CMD_KV_DUMP:
            return NVME_SUCCESS;
        default:
            return NVME_INVALID_OPCODE | NVME_DNR;
    }
}

static uint16_t pink_admin_cmd(FemuCtrl *n, NvmeCmd *cmd)
{
    switch (cmd->opcode) {
    case NVME_ADM_CMD_FEMU_FLIP:
        pink_flip(n, cmd);
        return NVME_SUCCESS;
    default:
        return NVME_INVALID_OPCODE | NVME_DNR;
    }
}

int nvme_register_pinkssd_exp(FemuCtrl *n)
{
    n->ext_ops = (FemuExtCtrlOps) {
        .state            = NULL,
        .init             = pink_init,
        .exit             = NULL,
        .rw_check_req     = NULL,
        .admin_cmd        = pink_admin_cmd,
        .io_cmd           = pink_io_cmd,
        .get_log          = NULL,
    };

    return 0;
}

