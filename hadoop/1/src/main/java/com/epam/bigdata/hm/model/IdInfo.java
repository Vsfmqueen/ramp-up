package com.epam.bigdata.hm.model;

public class IdInfo {
    private String ip;
    private Long count;

    public IdInfo() {
    }

    public IdInfo(String ip, Long count) {
        this.ip = ip;
        this.count = count;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IdInfo)) {
            return false;
        }

        IdInfo idInfo = (IdInfo) o;

        if (!count.equals(idInfo.count)) {
            return false;
        }
        if (!ip.equals(idInfo.ip)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return ip + " " + count;
    }
}
