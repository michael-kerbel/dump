package util.dump.externalization;

import java.util.Random;

import util.dump.ExternalizableBean;


public class DumpOfferBeanExternalizableBeanFields implements ExternalizableBean, Comparable<DumpOfferBeanExternalizableBeanFields> {

   private static final long   RANDOM_SEED = 123456;
   private static final Random RANDOM      = new Random(RANDOM_SEED);


   public static DumpOfferBeanExternalizableBeanFields createRandomBean() {
      DumpOfferBeanExternalizableBeanFields d = new DumpOfferBeanExternalizableBeanFields();
      d.code = randomString();
      d.codeType = RANDOM.nextInt();
      d.contentId = RANDOM.nextLong();
      d.created = RANDOM.nextInt();
      d.deeplink = randomString();
      d.deliveryCost = randomString();
      d.deliveryCostValue = RANDOM.nextDouble();
      d.deliveryTime = randomString();
      d.deliveryTimeColor = RANDOM.nextInt();
      d.description = randomString();
      d.hid = RANDOM.nextLong();
      d.imageSmall = randomString();
      d.imageView = randomString();
      d.isActive = RANDOM.nextInt() % 2 == 0 ? true : false;
      d.manufacturer = randomString();
      d.manufacturerPrdId = randomString();
      d.matchOwner = RANDOM.nextLong();
      d.matchProgram = RANDOM.nextLong();
      d.matchStamp = RANDOM.nextLong();
      d.matchType = RANDOM.nextLong();
      d.name = randomString();
      d.oldPrice = RANDOM.nextDouble();
      d.ownerId = RANDOM.nextLong();
      d.parentHid = RANDOM.nextLong();
      d.price = RANDOM.nextDouble();
      d.promoText = randomString();
      d.sourceGroup = randomString();
      d.sourceId = randomString();
      d.updated = RANDOM.nextLong();
      d.variant = RANDOM.nextLong();
      return d;
   }

   private static String randomString() {
      int length = RANDOM.nextInt(50);
      StringBuilder sb = new StringBuilder(length);
      for ( int i = 0; i < length; i++ ) {
         sb.append((char)(RANDOM.nextInt(64) + 32));
      }
      return sb.toString();
   }


   @externalize(1)
   private long    contentId;
   @externalize(2)
   private long    ownerId;
   @externalize(3)
   private String  sourceId;
   @externalize(4)
   private String  sourceGroup;

   @externalize(5)
   private boolean isActive;

   @externalize(6)
   private long    created;
   @externalize(7)
   private long    updated;

   @externalize(8)
   private long    hid;
   @externalize(9)
   private long    parentHid;
   @externalize(10)
   private long    variant;

   @externalize(11)
   private long    matchOwner;
   @externalize(12)
   private long    matchProgram;
   @externalize(13)
   private long    matchType;
   @externalize(14)
   private long    matchStamp;

   @externalize(15)
   private String  name;
   @externalize(16)
   private String  description;
   @externalize(17)
   private String  manufacturer;
   @externalize(18)
   private String  manufacturerPrdId;
   @externalize(19)
   private String  promoText;

   @externalize(20)
   private double  price;
   @externalize(21)
   private Double  oldPrice;

   @externalize(22)
   private String  code;
   @externalize(23)
   private int     codeType;

   @externalize(24)
   private String  deeplink;

   @externalize(25)
   private String  deliveryTime;
   @externalize(26)
   private int     deliveryTimeColor;
   @externalize(27)
   private String  deliveryCost;
   @externalize(28)
   private Double  deliveryCostValue;

   @externalize(29)
   private String  imageView;
   @externalize(30)
   private String  imageSmall;


   public DumpOfferBeanExternalizableBeanFields() {}

   @Override
   public int compareTo( DumpOfferBeanExternalizableBeanFields o ) {
      return this.contentId < o.contentId ? -1 : (this.contentId == o.contentId ? 0 : 1);
   }

   @Override
   public boolean equals( Object obj ) {
      if ( this == obj ) {
         return true;
      }
      if ( obj == null ) {
         return false;
      }
      if ( getClass() != obj.getClass() ) {
         return false;
      }
      DumpOfferBeanExternalizableBeanFields other = (DumpOfferBeanExternalizableBeanFields)obj;
      if ( code == null ) {
         if ( other.code != null ) {
            return false;
         }
      } else if ( !code.equals(other.code) ) {
         return false;
      }
      if ( codeType != other.codeType ) {
         return false;
      }
      if ( contentId != other.contentId ) {
         return false;
      }
      if ( created != other.created ) {
         return false;
      }
      if ( deeplink == null ) {
         if ( other.deeplink != null ) {
            return false;
         }
      } else if ( !deeplink.equals(other.deeplink) ) {
         return false;
      }
      if ( deliveryCost == null ) {
         if ( other.deliveryCost != null ) {
            return false;
         }
      } else if ( !deliveryCost.equals(other.deliveryCost) ) {
         return false;
      }
      if ( deliveryCostValue == null ) {
         if ( other.deliveryCostValue != null ) {
            return false;
         }
      } else if ( !deliveryCostValue.equals(other.deliveryCostValue) ) {
         return false;
      }
      if ( deliveryTime == null ) {
         if ( other.deliveryTime != null ) {
            return false;
         }
      } else if ( !deliveryTime.equals(other.deliveryTime) ) {
         return false;
      }
      if ( deliveryTimeColor != other.deliveryTimeColor ) {
         return false;
      }
      if ( description == null ) {
         if ( other.description != null ) {
            return false;
         }
      } else if ( !description.equals(other.description) ) {
         return false;
      }
      if ( hid != other.hid ) {
         return false;
      }
      if ( imageSmall == null ) {
         if ( other.imageSmall != null ) {
            return false;
         }
      } else if ( !imageSmall.equals(other.imageSmall) ) {
         return false;
      }
      if ( imageView == null ) {
         if ( other.imageView != null ) {
            return false;
         }
      } else if ( !imageView.equals(other.imageView) ) {
         return false;
      }
      if ( isActive != other.isActive ) {
         return false;
      }
      if ( manufacturer == null ) {
         if ( other.manufacturer != null ) {
            return false;
         }
      } else if ( !manufacturer.equals(other.manufacturer) ) {
         return false;
      }
      if ( manufacturerPrdId == null ) {
         if ( other.manufacturerPrdId != null ) {
            return false;
         }
      } else if ( !manufacturerPrdId.equals(other.manufacturerPrdId) ) {
         return false;
      }
      if ( matchOwner != other.matchOwner ) {
         return false;
      }
      if ( matchProgram != other.matchProgram ) {
         return false;
      }
      if ( matchStamp != other.matchStamp ) {
         return false;
      }
      if ( matchType != other.matchType ) {
         return false;
      }
      if ( name == null ) {
         if ( other.name != null ) {
            return false;
         }
      } else if ( !name.equals(other.name) ) {
         return false;
      }
      if ( oldPrice == null ) {
         if ( other.oldPrice != null ) {
            return false;
         }
      } else if ( !oldPrice.equals(other.oldPrice) ) {
         return false;
      }
      if ( ownerId != other.ownerId ) {
         return false;
      }
      if ( parentHid != other.parentHid ) {
         return false;
      }
      if ( Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price) ) {
         return false;
      }
      if ( promoText == null ) {
         if ( other.promoText != null ) {
            return false;
         }
      } else if ( !promoText.equals(other.promoText) ) {
         return false;
      }
      if ( sourceGroup == null ) {
         if ( other.sourceGroup != null ) {
            return false;
         }
      } else if ( !sourceGroup.equals(other.sourceGroup) ) {
         return false;
      }
      if ( sourceId == null ) {
         if ( other.sourceId != null ) {
            return false;
         }
      } else if ( !sourceId.equals(other.sourceId) ) {
         return false;
      }
      if ( updated != other.updated ) {
         return false;
      }
      if ( variant != other.variant ) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((code == null) ? 0 : code.hashCode());
      result = prime * result + codeType;
      result = prime * result + (int)(contentId ^ (contentId >>> 32));
      result = prime * result + (int)(created ^ (created >>> 32));
      result = prime * result + ((deeplink == null) ? 0 : deeplink.hashCode());
      result = prime * result + ((deliveryCost == null) ? 0 : deliveryCost.hashCode());
      result = prime * result + ((deliveryCostValue == null) ? 0 : deliveryCostValue.hashCode());
      result = prime * result + ((deliveryTime == null) ? 0 : deliveryTime.hashCode());
      result = prime * result + deliveryTimeColor;
      result = prime * result + ((description == null) ? 0 : description.hashCode());
      result = prime * result + (int)(hid ^ (hid >>> 32));
      result = prime * result + ((imageSmall == null) ? 0 : imageSmall.hashCode());
      result = prime * result + ((imageView == null) ? 0 : imageView.hashCode());
      result = prime * result + (isActive ? 1231 : 1237);
      result = prime * result + ((manufacturer == null) ? 0 : manufacturer.hashCode());
      result = prime * result + ((manufacturerPrdId == null) ? 0 : manufacturerPrdId.hashCode());
      result = prime * result + (int)(matchOwner ^ (matchOwner >>> 32));
      result = prime * result + (int)(matchProgram ^ (matchProgram >>> 32));
      result = prime * result + (int)(matchStamp ^ (matchStamp >>> 32));
      result = prime * result + (int)(matchType ^ (matchType >>> 32));
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((oldPrice == null) ? 0 : oldPrice.hashCode());
      result = prime * result + (int)(ownerId ^ (ownerId >>> 32));
      result = prime * result + (int)(parentHid ^ (parentHid >>> 32));
      long temp;
      temp = Double.doubleToLongBits(price);
      result = prime * result + (int)(temp ^ (temp >>> 32));
      result = prime * result + ((promoText == null) ? 0 : promoText.hashCode());
      result = prime * result + ((sourceGroup == null) ? 0 : sourceGroup.hashCode());
      result = prime * result + ((sourceId == null) ? 0 : sourceId.hashCode());
      result = prime * result + (int)(updated ^ (updated >>> 32));
      result = prime * result + (int)(variant ^ (variant >>> 32));
      return result;
   }
}
